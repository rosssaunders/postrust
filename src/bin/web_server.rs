use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};

fn main() {
    let mut args = std::env::args().skip(1);
    let port = args
        .next()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8080);

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });

    println!("serving ./web at http://{addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(err) = handle_connection(stream, Path::new("web")) {
                    eprintln!("request error: {err}");
                }
            }
            Err(err) => eprintln!("connection error: {err}"),
        }
    }
}

fn handle_connection(mut stream: TcpStream, root: &Path) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_path = parts.next().unwrap_or("/");

    if method != "GET" && method != "HEAD" {
        return write_response(
            &mut stream,
            405,
            "Method Not Allowed",
            "text/plain; charset=utf-8",
            b"Only GET and HEAD are supported.\n",
            method == "HEAD",
        );
    }

    let file_path = resolve_path(root, raw_path);
    let file_path = if file_path.is_dir() {
        file_path.join("index.html")
    } else {
        file_path
    };

    match fs::read(&file_path) {
        Ok(body) => {
            let content_type = content_type(&file_path);
            write_response(
                &mut stream,
                200,
                "OK",
                content_type,
                &body,
                method == "HEAD",
            )
        }
        Err(_) => write_response(
            &mut stream,
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"Not Found\n",
            method == "HEAD",
        ),
    }
}

fn resolve_path(root: &Path, raw_path: &str) -> PathBuf {
    let path_without_query = raw_path.split('?').next().unwrap_or("/");
    let mut full = PathBuf::from(root);
    let trimmed = path_without_query.trim_start_matches('/');

    if trimmed.is_empty() {
        return full.join("index.html");
    }

    for component in Path::new(trimmed).components() {
        if let Component::Normal(value) = component {
            full.push(value);
        }
    }
    full
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|s| s.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("map") => "application/json; charset=utf-8",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    status_text: &str,
    content_type: &str,
    body: &[u8],
    head_only: bool,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;

    if !head_only {
        stream.write_all(body)?;
    }
    stream.flush()
}
