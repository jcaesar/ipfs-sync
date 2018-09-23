extern crate ipfsapi;
extern crate failure;
extern crate humantime;
extern crate pathdiff;
#[macro_use] extern crate clap;

use ipfsapi::IpfsApi;
use ipfsapi::mfs;
use std::collections::HashSet;
use std::env;
use std::error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::process::exit;
use std::time::{ Duration, Instant, SystemTime, UNIX_EPOCH };
use std::os::unix::fs::MetadataExt;
use pathdiff::diff_paths;

pub type Fallible<T> = Result<T, failure::Error>;

#[derive(Debug)]
struct RTError { subject: String, }
impl RTError {
    fn new(subject: &str) -> RTError {
        RTError { subject: subject.to_string() }
    }
}
impl error::Error for RTError {
    fn description(&self) -> &str {
        "Could not parse filename as unicode"
    }
}
impl fmt::Display for RTError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse file name {:#?} as unicode", self.subject)
    }
}

struct Env<'a> {
    verbosity: u64,
    flush: &'a mut FnMut() -> Fallible<()>,
    nocopy: bool,
    syncfrom: Option<i64>, // unix file system timestamp as returned by ctime
}

fn main() {
    let start_time = SystemTime::now();

	let matches = clap_app!(myapp =>
		(version: "0.3")
		(author: "Julius Michaelis <jcipfs@liftm.de>")
		(about: "Sync a local folder to an MFS folder based on file existence and size")
		(@arg src: -s --src +takes_value +required "source path")
		(@arg dst: -d --dst +takes_value +required "destination path")
		(@arg apihost: -h --apihost +takes_value "api host - defaults to localhost")
		(@arg apiport: -p --apiport +takes_value "destination path - defaults to 5001")
		(@arg flushivl: -f --flush +takes_value "flush interval - only one final flush will be executed if unset")
		(@arg syncfrom: -a --after +takes_value "sync if the file change time is any later than the given date - only existence will be checked otherwise")
		(@arg syncff: -t --tsfile +takes_value "read value for file change time limit from file, and write file upon successful sync")
		(@arg nocopy: -l --nocopy "Use the filestore")
		(@arg verbose: -v --verbose ... "Verbosity")
	).get_matches();

    let arg = |name| matches.value_of(name).unwrap();
    let argdef = |name, def| matches.value_of(name).unwrap_or(def);

    let verbosity = matches.occurrences_of("verbose");

    let api = IpfsApi::new(
        argdef("apihost", "127.0.0.1"), 
        argdef("apiport", "5001").parse::<u16>().expect("Could not parse IPFS API port")
    );

    let flushivl: Option<Duration> = matches.value_of("flushivl")
            .map(|ivl| ivl.parse::<humantime::Duration>().expect("Could not parse flush interval").into());

    let syncff = matches.value_of("syncff").map(|ff| fs::canonicalize(ff).expect("Could not get absolute path of sync timestamp file"));
    let syncfrom = {
        if let Some(date) = matches.value_of("syncfrom") {
            let msg = "Could not parse change time";
            let parse = date.parse::<humantime::Timestamp>().map(|t| -> SystemTime { t.into() });
            Some(match parse {
                Ok(t) => t.duration_since(UNIX_EPOCH).expect(msg).as_secs() as i64,
                e => {
                    if date.starts_with("@") { date[1..].parse::<i64>().expect(msg) }
                    else { e.expect(msg); panic!("unreachable") }
                }
            })
        } else if let Some(ref ff) = syncff {
            match (|| -> Fallible<i64> {
                let ffs = fs::read_to_string(ff)?;
                Ok(ffs.parse::<i64>()?)
            })() {
                Ok(ts) => Some(ts),
                Err(err) => {
                    println!("Warning: error reading sync time limit from {}: {} - syncinc all.", ff.display(), err);
                    Some(0)
                }
            }
        } else {
            None
        }
    };

    let nocopy = matches.is_present("nocopy");

    match (|| -> Fallible<(String, u64)> {
        env::set_current_dir(PathBuf::from(arg("src")))?;
        let dst = api.mfs()
            .autoflush(flushivl.map(|ivl| ivl <= Duration::from_secs(0)).unwrap_or(false))
            .cd(arg("dst"));
        let flushdst = dst.cd(".");
        let mut nextflush = Instant::now();
        let mut flush = || {
            if let Some(flushivl) = flushivl {
                let now = Instant::now();
                if now > nextflush {
                    flushdst.flush()?;
                    nextflush = now + flushivl;
                }
            }
            Ok(())
        };
        let mut env = Env {
            verbosity: verbosity,
            flush: &mut flush,
            nocopy: nocopy,
            syncfrom: syncfrom,
        };
        let mut errs = 0;
        let symlinks = re_curse(PathBuf::from(".").canonicalize()?, dst.cd("."), &mut env, &mut errs)?;
        dst.flush()?;
        if verbosity >= 2 && !symlinks.is_empty() {
            println!("Installing {} symlinks as copies", symlinks.len());
        }
        for symlink in symlinks {
            let (from, to) = symlink;
            let from = from.to_str().ok_or(RTError::new("could not parse symlink source as unicode"))?;
            let to = to.to_str().ok_or(RTError::new("could not parse symlink destination as unicode"))?;
            if verbosity >= 2 {
                println!("{} → {}", from, to);
            }
            let from = dst.cd(from);
            let to = from.cd(to);
            match to.stat() {
                Ok(stat) => {
                    if let Ok(fstat) = from.stat() {
                        if fstat.Hash == stat.Hash {
                            continue
                        }
                    }
                    if verbosity >= 1 {
                        println!("{} → {}", stat.Hash, from.cwd());
                    }
                    from.cpf(&stat.Hash)?;
                },
                Err(err) => {
                     println!("Could resolve symlink from {} to {} as copy: statting source: {}", from.cwd(), to.cwd(), err);
                     errs += 1;
                }
            }
        }
        dst.flush()?;
        Ok((dst.stat()?.Hash, errs))
    })() {
        Ok((hash, 0)) => {
            if let Some(ref ff) = syncff {
                let tss = start_time
                    .duration_since(UNIX_EPOCH)
                    .expect("Could not calculate current UNIX time")
                    .as_secs().to_string();
                fs::write(ff, tss)
                    .map_err(|err| println!("Warning: error writing sync timestamp: {}", err)).ok();
            };
            let dur = SystemTime::now().duration_since(start_time).expect("Could not calculate execution time");
            println!("Success in {}: {}", humantime::Duration::from(dur), hash);
            exit(0)
        },
        Ok((hash, n)) => {
            println!("Success with {} errors: {}", hash, n);
            exit(1)
        },
        Err(err) => {
            println!("Error: {}", err);
            exit(-1)
        }
    }
}

type Symlinks = Vec<(PathBuf, PathBuf)>;
fn re_curse(dir: PathBuf, mfs: mfs::MFS, env: &mut Env, errs: &mut u64) -> Fallible<Symlinks> {
    let mut ret : Symlinks = vec![];
    if env.verbosity >= 2 {
        println!("Entering {}", mfs.cwd());
    }
    let mut mfsents : HashSet<String> = (|| {
        match mfs.ls() {
            Err(_err) => {
                if env.verbosity >= 3 {
                    println!("Error on initial listing of {}: {}", mfs.cwd(), _err)
                }
                mfs.rm().ok();
                mfs.mkdir()?;
                if env.verbosity >= 1 {
                    println!("{} → {}", mfs.stat()?.Hash, mfs.cwd());
                }
                Ok(vec![])
            }
            ok => ok
        }
    })()?.into_iter().collect();
    for dent in fs::read_dir(dir)?.filter_map(|e| e.ok()) { if let Err(err) = (|| -> Fallible<()> {
        let dp = dent.path();
        let ft = dent.file_type()?;
        let name = dent.file_name();
        let name = name.to_str().ok_or(RTError::new("could not parse filename as unicode"))?;
        let existed = mfsents.remove(name);
        if ft.is_symlink() {
            let src = diff_paths(&dp, &std::env::current_dir()?).ok_or(RTError::new("Could not get relative path for symlink source"))?;
            let dst = diff_paths(&dp.canonicalize()?, &dp).ok_or(RTError::new("Could not get relative path for symlink destination"))?;
            if env.verbosity >= 2 {
                println!("Postponing symlink: {} → {}", dp.display(), dst.display());
            }
            ret.push((src, dst));
        } else if ft.is_dir() {
            let mut symlinks = re_curse(dent.path(), mfs.cd(&name), env, errs)?;
            ret.append(&mut symlinks);
        } else {
            if !existed || {
                if let Some(syncfrom) = env.syncfrom {
                    fs::metadata(&dp)?.ctime() > syncfrom
                } else {
                    false
                }
            } {
                let mut add = mfs.api.add();
                let add = add.pin(false);
                let hash = if env.nocopy {
                    let add = add.nocopy(true);
                    add.from_path(&dp)
                } else {
                    let file = fs::File::open(&dp)?;
                    add.read_from(file)
                } ?;
                let mfs = mfs.cd(name);
                mfs.cpf(&hash)?;
                if env.verbosity >= 1 {
                    println!("{} → {}", hash, mfs.cwd());
                }
                (env.flush)()?
            }
        }
        Ok(())
    }
    )() {
       println!("Error processing {:?}: {}", dent, err);
       *errs += 1;
    }; }
    for ment in mfsents {
        mfs.cd(&ment).rm()?;
    }
    Ok(ret)
}
