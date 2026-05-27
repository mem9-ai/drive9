import Foundation
#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// Minimal HTTP/1.1 server for tests. Listens on a free port on 127.0.0.1 and
/// dispatches based on the literal "METHOD path?query" key. Just enough to
/// drive the SDK smoke tests; not a general-purpose server.
final class MockHTTPServer: @unchecked Sendable {
    struct Request {
        let method: String
        /// Full request target including query string.
        let path: String
        /// Path portion only (everything before `?`).
        let pathOnly: String
        /// Query string (excluding the `?`).
        let query: String
        let headers: [String: String]
        let body: Data
    }

    typealias Handler = (Request) -> MockResponse

    private(set) var port: Int = 0
    /// Routes keyed by "METHOD path?query" — match the full target including
    /// query string. Used when the test cares about the exact URL.
    private var routes: [String: Handler] = [:]
    /// Routes keyed by "METHOD pathOnly" — match any query string. Used when
    /// HashMap-ordered query params are non-deterministic and the handler
    /// inspects `Request.query` itself.
    private var pathOnlyRoutes: [String: Handler] = [:]
    private var serverSocket: Int32 = -1
    private var acceptThread: Thread?
    private let lock = NSLock()
    private var stopped = false

    var baseURL: String { "http://127.0.0.1:\(port)" }

    init() throws {}

    func route(_ method: String, _ path: String, handler: @escaping Handler) {
        lock.lock()
        routes["\(method) \(path)"] = handler
        lock.unlock()
    }

    /// Register a handler that matches any query string for the given method
    /// and path. The handler can inspect `Request.query` to assert specifics.
    func routeAnyQuery(_ method: String, _ pathOnly: String, handler: @escaping Handler) {
        lock.lock()
        pathOnlyRoutes["\(method) \(pathOnly)"] = handler
        lock.unlock()
    }

    func start() throws {
        #if canImport(Glibc)
        let socketType = Int32(SOCK_STREAM.rawValue)
        #else
        let socketType = SOCK_STREAM
        #endif
        let sock = socket(AF_INET, socketType, 0)
        guard sock >= 0 else { throw POSIXError(.EIO) }
        var yes: Int32 = 1
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_addr.s_addr = inet_addr("127.0.0.1")
        addr.sin_port = 0
        let bindResult = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                bind(sock, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindResult == 0 else {
            close(sock)
            throw POSIXError(.EADDRINUSE)
        }
        guard listen(sock, 16) == 0 else {
            close(sock)
            throw POSIXError(.EIO)
        }
        var bound = sockaddr_in()
        var boundLen = socklen_t(MemoryLayout<sockaddr_in>.size)
        withUnsafeMutablePointer(to: &bound) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                _ = getsockname(sock, ptr, &boundLen)
            }
        }
        port = Int(UInt16(bigEndian: bound.sin_port))
        serverSocket = sock

        let thread = Thread { [weak self] in
            self?.acceptLoop()
        }
        thread.name = "MockHTTPServer-accept"
        thread.start()
        acceptThread = thread
    }

    func stop() {
        lock.lock()
        stopped = true
        lock.unlock()
        if serverSocket >= 0 {
            shutdown(serverSocket, Int32(SHUT_RDWR))
            close(serverSocket)
            serverSocket = -1
        }
        acceptThread?.cancel()
        acceptThread = nil
    }

    private func acceptLoop() {
        while true {
            lock.lock()
            let done = stopped
            lock.unlock()
            if done { return }

            var clientAddr = sockaddr_in()
            var len = socklen_t(MemoryLayout<sockaddr_in>.size)
            let client = withUnsafeMutablePointer(to: &clientAddr) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                    accept(serverSocket, ptr, &len)
                }
            }
            if client < 0 { return }
            handle(client: client)
            close(client)
        }
    }

    private func handle(client: Int32) {
        guard let request = readRequest(from: client) else { return }
        let exactKey = "\(request.method) \(request.path)"
        let pathOnlyKey = "\(request.method) \(request.pathOnly)"
        lock.lock()
        let handler = routes[exactKey] ?? pathOnlyRoutes[pathOnlyKey]
        lock.unlock()
        let response: MockResponse
        if let handler = handler {
            response = handler(request)
        } else {
            response = MockResponse(status: 404, body: Data("no route for \(exactKey)".utf8))
        }
        sendResponse(response, to: client)
    }

    private func readRequest(from client: Int32) -> Request? {
        var buffer = Data()
        var headerEnd: Range<Data.Index>?
        var chunk = [UInt8](repeating: 0, count: 4096)
        while headerEnd == nil {
            let n = recv(client, &chunk, chunk.count, 0)
            if n <= 0 { return nil }
            buffer.append(contentsOf: chunk[0..<n])
            headerEnd = buffer.range(of: Data("\r\n\r\n".utf8))
        }
        let headerData = buffer.subdata(in: 0..<headerEnd!.lowerBound)
        guard let headerString = String(data: headerData, encoding: .utf8) else { return nil }
        let lines = headerString.split(separator: "\r\n", omittingEmptySubsequences: false)
        guard let requestLine = lines.first else { return nil }
        let parts = requestLine.split(separator: " ")
        guard parts.count >= 2 else { return nil }
        let method = String(parts[0])
        let path = String(parts[1])

        var contentLength = 0
        var headers: [String: String] = [:]
        for line in lines.dropFirst() {
            let segments = line.split(separator: ":", maxSplits: 1)
            if segments.count == 2 {
                let name = String(segments[0])
                let value = String(segments[1].trimmingCharacters(in: .whitespaces))
                headers[name.lowercased()] = value
                if name.lowercased() == "content-length" {
                    contentLength = Int(value) ?? 0
                }
            }
        }

        var body = buffer.subdata(in: headerEnd!.upperBound..<buffer.count)
        while body.count < contentLength {
            let n = recv(client, &chunk, chunk.count, 0)
            if n <= 0 { break }
            body.append(contentsOf: chunk[0..<n])
        }
        if body.count > contentLength {
            body = body.subdata(in: 0..<contentLength)
        }
        let questionMarkIndex = path.firstIndex(of: "?")
        let pathOnly = questionMarkIndex.map { String(path[..<$0]) } ?? path
        let query = questionMarkIndex.map { String(path[path.index(after: $0)...]) } ?? ""
        return Request(method: method, path: path, pathOnly: pathOnly, query: query, headers: headers, body: body)
    }

    private func sendResponse(_ response: MockResponse, to client: Int32) {
        var headers = "HTTP/1.1 \(response.status) OK\r\n"
        let overridesContentLength = response.extraHeaders.keys
            .contains { $0.lowercased() == "content-length" }
        if !overridesContentLength {
            headers += "Content-Length: \(response.body.count)\r\n"
        }
        if let ct = response.contentType {
            headers += "Content-Type: \(ct)\r\n"
        }
        for (k, v) in response.extraHeaders {
            headers += "\(k): \(v)\r\n"
        }
        headers += "Connection: close\r\n\r\n"
        let headerData = Data(headers.utf8)
        writeAll(headerData, to: client)
        writeAll(response.body, to: client)
    }

    private func writeAll(_ data: Data, to client: Int32) {
        data.withUnsafeBytes { raw in
            guard let base = raw.bindMemory(to: UInt8.self).baseAddress else { return }
            var sentBytes = 0
            while sentBytes < data.count {
                let n = send(client, base.advanced(by: sentBytes), data.count - sentBytes, 0)
                if n < 0 && errno == EINTR { continue }
                if n <= 0 { return }
                sentBytes += n
            }
        }
    }
}

struct MockResponse {
    var status: Int
    var body: Data
    var contentType: String?
    var extraHeaders: [String: String] = [:]
}
