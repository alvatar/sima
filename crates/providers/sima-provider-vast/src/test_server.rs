//! A scripted HTTP server the crate's tests point the backend at.
//!
//! It answers requests from a fixed script, in order, and records what it
//! received, so a test states the marketplace's answers and then asserts on
//! the request the backend built. Serving is sequential: one connection is
//! read and answered before the next is accepted, which matches a client
//! whose calls are synchronous.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// One scripted answer: the status line's code and the body verbatim.
pub(crate) struct ScriptedAnswer {
    /// The HTTP status to answer with.
    pub(crate) status: u16,
    /// The response body, sent as `application/json`.
    pub(crate) body: String,
}

/// A request the server received.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    /// The HTTP method.
    pub(crate) method: String,
    /// The request target, query string included.
    pub(crate) path: String,
    /// The `Authorization` header, when the client sent one.
    pub(crate) authorization: Option<String>,
    /// The request body, empty when there was none.
    pub(crate) body: String,
}

impl RecordedRequest {
    /// The body parsed as JSON, for asserting on the fields a request
    /// carried.
    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).expect("request body is JSON")
    }
}

/// A server answering `answers` in order and recording every request.
pub(crate) struct TestServer {
    /// The base URL a backend is configured with to reach this server.
    url: String,
    /// Requests received so far, shared with the serving thread.
    received: Arc<Mutex<Vec<RecordedRequest>>>,
    /// The serving thread, joined when the server is dropped.
    serving: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Starts a server on a loopback port that answers `answers` in order,
    /// then stops. A request past the script's end is never answered, which
    /// surfaces as the test's client failing rather than as a silent extra
    /// call.
    pub(crate) fn new(answers: Vec<ScriptedAnswer>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback port");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let received = Arc::new(Mutex::new(Vec::new()));
        let recording = Arc::clone(&received);
        let serving = std::thread::spawn(move || {
            for answer in answers {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let request = read_request(&stream);
                recording.lock().expect("recorded requests").push(request);
                write_answer(stream, &answer);
            }
        });
        TestServer {
            url,
            received,
            serving: Some(serving),
        }
    }

    /// The base URL to configure the backend with.
    pub(crate) fn url(&self) -> String {
        self.url.clone()
    }

    /// The requests received so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.received.lock().expect("recorded requests").clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // A script the test left unspent leaves the thread blocked on
        // `accept`, so the join happens only once every answer is served.
        if let Some(serving) = self.serving.take()
            && serving.is_finished()
        {
            let _ = serving.join();
        }
    }
}

/// Reads one HTTP request off `stream`: the request line, the headers, and
/// as many body bytes as `Content-Length` announces.
fn read_request(stream: &TcpStream) -> RecordedRequest {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).expect("request line");
    let mut fields = request_line.split_whitespace();
    let method = fields.next().unwrap_or_default().to_string();
    let path = fields.next().unwrap_or_default().to_string();
    let mut authorization = None;
    let mut length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).expect("header line");
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let Some((name, value)) = header.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "authorization" => authorization = Some(value.to_string()),
            "content-length" => length = value.parse().unwrap_or(0),
            _ => {}
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).expect("request body");
    RecordedRequest {
        method,
        path,
        authorization,
        body: String::from_utf8(body).expect("request body is UTF-8"),
    }
}

/// Writes `answer` to `stream` and closes it.
fn write_answer(mut stream: TcpStream, answer: &ScriptedAnswer) {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        answer.status,
        reason(answer.status),
        answer.body.len(),
        answer.body
    );
    stream.write_all(response.as_bytes()).expect("write answer");
    stream.flush().expect("flush answer");
}

/// The status line's reason phrase for the statuses the marketplace uses.
fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        410 => "Gone",
        500 => "Internal Server Error",
        _ => "Status",
    }
}
