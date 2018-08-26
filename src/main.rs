extern crate ipfsapi;
extern crate failure;
extern crate humantime;
#[macro_use] extern crate clap;

use ipfsapi::IpfsApi;
use ipfsapi::mfs;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::process::exit;
use std::time;

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
}

fn main() {
	let matches = clap_app!(myapp =>
		(version: "0.3")
		(author: "Julius Michaelis <jcipfs@liftm.de>")
		(about: "Sync a local folder to an MFS folder based on file existence and size")
		(@arg src: -s --src +takes_value +required "source path")
		(@arg dst: -d --dst +takes_value +required "destination path")
		(@arg apihost: -h --apihost +takes_value "api host - defaults to localhost")
		(@arg apiport: -p --apiport +takes_value "destination path - defaults to 5001")
		(@arg flushivl: -f --flush +takes_value "flush interval - only one final flush will be executed if unset")
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

    let flushivl: Option<time::Duration> = matches.value_of("flushivl") 
            .map(|ivl| ivl.parse::<humantime::Duration>().expect("Could not parse flush interval").into());

    let nocopy = matches.is_present("nocopy");
 
    match (|| -> Fallible<String> {
        let src = PathBuf::from(arg("src"));
        let src = if nocopy {
            fs::canonicalize(&src)?
        } else { src };
        let dst = api.mfs()
            .autoflush(flushivl.map(|ivl| ivl <= time::Duration::from_secs(0)).unwrap_or(false))
            .cd(arg("dst"));
        let flushdst = dst.cd(".");
        let mut nextflush = time::Instant::now();
        let mut flush = || {
            if let Some(flushivl) = flushivl {
                let now = time::Instant::now();
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
        };
        re_curse(src, dst.cd("."), &mut env)?;
        dst.flush()?;
        Ok(dst.stat()?.Hash)
    })() {
        Ok(hash) => {
            println!("{}", hash);
            exit(0)
        },
        Err(err) => {
            println!("Error: {}", err);
            exit(-1)
        }
    }
}

fn re_curse(dir: PathBuf, mfs: mfs::MFS, env: &mut Env) -> Fallible<()> {
    if env.verbosity >= 2 {
        println!("Entering {}", mfs.cwd());
    }
    let mut mfsents : HashMap<String, mfs::MfsNode> = (|| {
        match mfs.ls() {
            Err(_err) => {
                mfs.rm().ok();
                mfs.mkdir()?;
                if env.verbosity >= 1 {
                    println!("{} → {}", mfs.stat()?.Hash, mfs.cwd());
                }
                Ok(vec![])
            }
            ok => ok
        }
    })()?.into_iter().map(|e| (e.name.clone(), e)).collect();
    for dent in fs::read_dir(dir)?.filter_map(|e| e.ok()) { if let Err(err) = (|| -> Fallible<()> { 
        let dp = dent.path();
        let _dpp = dp.display();
        let ft = dent.file_type()?;
        let name = dent.file_name();
        let name = name.to_str().ok_or(RTError::new("could not parse filename as unicode"))?;
        if ft.is_dir() {
            re_curse(dent.path(), mfs.cd(&name), env)?;
            mfsents.remove(name);
        } else {
            let md = dent.metadata()?;
            if match mfsents.remove(name) {
                Some (ment) => ment.size != md.len(),
                None => true
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
       println!("Error processing {:?}: {}", dent, err)
    }; }
    for (ment, _) in mfsents {
        mfs.cd(&ment).rmr()?;
    }
    Ok(())
}
