import XCTest
@testable import Drive9Mobile

final class ReceivedBody: @unchecked Sendable {
    private var body = Data()
    private let lock = NSLock()
    func set(_ value: Data) { lock.lock(); body = value; lock.unlock() }
    func get() -> Data { lock.lock(); defer { lock.unlock() }; return body }
}

final class HitCounter: @unchecked Sendable {
    private var count = 0
    private let lock = NSLock()
    func bump() { lock.lock(); count += 1; lock.unlock() }
    func get() -> Int { lock.lock(); defer { lock.unlock() }; return count }
}

final class RecordingProgressListener: Drive9ProgressListener, @unchecked Sendable {
    func onProgress(transferred: UInt64, total: UInt64) {}
}

final class Drive9Tests: XCTestCase {
    var server: MockHTTPServer!

    override func setUp() async throws {
        server = try MockHTTPServer()
        try server.start()
    }

    override func tearDown() async throws {
        server.stop()
        server = nil
    }

    func testWriteThenReadRoundtripAndHeaders() async throws {
        server.route("PUT", "/v1/fs/hello.txt") { req in
            XCTAssertEqual(req.headers["authorization"], "Bearer test-key")
            XCTAssertEqual(req.headers["x-dat9-expected-revision"], "7")
            XCTAssertEqual(req.body, Data("hello swift".utf8))
            return MockResponse(status: 200, body: Data())
        }
        server.route("GET", "/v1/fs/hello.txt") { _ in
            MockResponse(status: 200, body: Data("hello swift".utf8))
        }

        let client = Drive9Client(baseUrl: server.baseURL + "/", apiKey: "test-key")
        try await client.write(path: "hello.txt", data: Data("hello swift".utf8), expectedRevision: 7)
        let data = try await client.read(path: "/hello.txt")
        XCTAssertEqual(data, Data("hello swift".utf8))
    }

    func testListReturnsEntries() async throws {
        server.route("GET", "/v1/fs/data/?list=1") { _ in
            let body = #"{"entries":[{"name":"a.txt","size":3,"isDir":false},{"name":"sub","size":0,"isDir":true}]}"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let entries = try await client.list(path: "/data/")
        XCTAssertEqual(entries.count, 2)
        XCTAssertEqual(entries[0].name, "a.txt")
        XCTAssertEqual(entries[0].size, 3)
        XCTAssertFalse(entries[0].isDir)
        XCTAssertTrue(entries[1].isDir)
    }

    func testStatReportsRevisionAndSize() async throws {
        server.route("HEAD", "/v1/fs/f.bin") { _ in
            MockResponse(
                status: 200,
                body: Data(),
                extraHeaders: [
                    "Content-Length": "42",
                    "X-Dat9-Revision": "9",
                    "X-Dat9-IsDir": "false",
                ]
            )
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let stat = try await client.stat(path: "/f.bin")
        XCTAssertEqual(stat.size, 42)
        XCTAssertEqual(stat.revision, 9)
        XCTAssertFalse(stat.isDir)
    }

    func testDeleteCopyRenameMkdirSucceed() async throws {
        server.route("DELETE", "/v1/fs/gone.txt") { _ in
            MockResponse(status: 204, body: Data())
        }
        server.route("POST", "/v1/fs/dst.txt?copy") { req in
            XCTAssertEqual(req.headers["x-dat9-copy-source"], "/src.txt")
            return MockResponse(status: 200, body: Data())
        }
        server.route("POST", "/v1/fs/new.txt?rename") { req in
            XCTAssertEqual(req.headers["x-dat9-rename-source"], "/old.txt")
            return MockResponse(status: 200, body: Data())
        }
        server.route("POST", "/v1/fs/dir/?mkdir") { _ in
            MockResponse(status: 200, body: Data())
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        try await client.delete(path: "/gone.txt")
        try await client.copy(srcPath: "src.txt", dstPath: "/dst.txt")
        try await client.rename(oldPath: "/old.txt", newPath: "/new.txt")
        try await client.mkdir(path: "/dir/")
    }

    func testGrepReturnsSearchResultsWithEncodedQuery() async throws {
        server.route("GET", "/v1/fs/?grep=hello%20world&limit=3") { _ in
            let body = #"[{"path":"/a.txt","name":"a.txt","size_bytes":7,"score":0.5}]"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let hits = try await client.grep(query: "hello world", pathPrefix: "/", limit: 3)
        XCTAssertEqual(hits.count, 1)
        XCTAssertEqual(hits[0].path, "/a.txt")
        XCTAssertEqual(hits[0].sizeBytes, 7)
        XCTAssertEqual(hits[0].score, 0.5)
    }

    func testFindForwardsParams() async throws {
        server.routeAnyQuery("GET", "/v1/fs/data/") { request in
            XCTAssertTrue(request.query.contains("find="), "missing find=: \(request.query)")
            XCTAssertTrue(request.query.contains("type=file"), "missing type=file: \(request.query)")
            XCTAssertTrue(request.query.contains("limit=10"), "missing limit=10: \(request.query)")
            let body = #"[{"path":"/data/x.txt","name":"x.txt","size_bytes":1,"score":null}]"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let hits = try await client.find(pathPrefix: "/data/", params: ["type": "file", "limit": "10"])
        XCTAssertEqual(hits.count, 1)
        XCTAssertEqual(hits[0].path, "/data/x.txt")
    }

    func testSqlReturnsJsonStrings() async throws {
        server.route("POST", "/v1/sql") { req in
            XCTAssertTrue(String(data: req.body, encoding: .utf8)?.contains("SELECT path") == true)
            let body = #"[{"path":"/a.txt","size":10},{"path":"/b","size":0}]"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let rows = try await client.sql(query: "SELECT path, size FROM files")
        XCTAssertEqual(rows.count, 2)
        let row0 = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(rows[0].utf8)) as? [String: Any])
        let row1 = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(rows[1].utf8)) as? [String: Any])
        XCTAssertEqual(row0["path"] as? String, "/a.txt")
        XCTAssertEqual(row0["size"] as? Int, 10)
        XCTAssertEqual(row1["path"] as? String, "/b")
    }

    func testUploadFileSmallRoundtrip() async throws {
        let received = ReceivedBody()
        server.route("PUT", "/v1/fs/up.bin") { req in
            received.set(req.body)
            return MockResponse(status: 200, body: Data())
        }

        let local = FileManager.default.temporaryDirectory
            .appendingPathComponent("drive9-swift-upload-\(UUID().uuidString).bin")
        let payload = Data(repeating: UInt8(ascii: "x"), count: 100)
        try payload.write(to: local)
        defer { try? FileManager.default.removeItem(at: local) }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        try await client.uploadFile(localPath: local.path, remotePath: "/up.bin")
        XCTAssertEqual(received.get(), payload)
    }

    func testDownloadFileRoundtripAndPreservesExistingDestinationOnFailure() async throws {
        let body = Data(repeating: UInt8(ascii: "a"), count: 200_000)
        server.route("GET", "/v1/fs/big.bin") { _ in
            MockResponse(status: 200, body: body)
        }
        server.route("GET", "/v1/fs/fail.bin") { _ in
            MockResponse(status: 500, body: Data(#"{"error":"boom"}"#.utf8), contentType: "application/json")
        }

        let dest = FileManager.default.temporaryDirectory
            .appendingPathComponent("drive9-swift-download-\(UUID().uuidString).bin")
        defer { try? FileManager.default.removeItem(at: dest) }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        try await client.downloadFile(remotePath: "/big.bin", localPath: dest.path)
        XCTAssertEqual(try Data(contentsOf: dest), body)

        let original = Data("do not overwrite me".utf8)
        try original.write(to: dest)
        do {
            try await client.downloadFile(remotePath: "/fail.bin", localPath: dest.path)
            XCTFail("expected http_status")
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, _, _, _) = error else {
                XCTFail("unexpected variant: \(error)")
                return
            }
            XCTAssertEqual(code, "http_status")
            XCTAssertEqual(try Data(contentsOf: dest), original)
        }
    }

    func testConflictPreservesServerRevisionAndStatusErrorCarriesCode() async throws {
        server.route("PUT", "/v1/fs/r.txt") { _ in
            let body = #"{"error":"revision mismatch","server_revision":12}"#
            return MockResponse(status: 409, body: Data(body.utf8), contentType: "application/json")
        }
        server.route("GET", "/v1/fs/missing.txt") { _ in
            let body = #"{"error":"forbidden"}"#
            return MockResponse(status: 403, body: Data(body.utf8), contentType: "application/json")
        }

        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        do {
            try await client.write(path: "/r.txt", data: Data("x".utf8), expectedRevision: 7)
            XCTFail("expected conflict")
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, statusCode, _, serverRevision) = error else {
                XCTFail("unexpected variant: \(error)")
                return
            }
            XCTAssertEqual(code, "conflict")
            XCTAssertEqual(statusCode, 409)
            XCTAssertEqual(serverRevision, 12)
        }

        do {
            _ = try await client.read(path: "/missing.txt")
            XCTFail("expected forbidden")
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, statusCode, detail, _) = error else {
                XCTFail("unexpected variant: \(error)")
                return
            }
            XCTAssertEqual(code, "http_status")
            XCTAssertEqual(statusCode, 403)
            XCTAssertEqual(detail, "forbidden")
        }
    }

    func testNativeStreamFlowVaultAndCancelApis() async throws {
        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")

        let uploaded = ReceivedBody()
        server.route("POST", "/v2/uploads/initiate") { _ in
            MockResponse(status: 200, body: Data(#"{"upload_id":"u1","key":"k","part_size":5,"total_parts":1}"#.utf8), contentType: "application/json")
        }
        server.route("POST", "/v2/uploads/u1/presign") { _ in
            MockResponse(status: 200, body: Data(#"{"number":1,"url":"\#(self.server.baseURL)/upload-part","size":5,"headers":{}}"#.utf8), contentType: "application/json")
        }
        server.route("PUT", "/upload-part") { req in
            uploaded.set(req.body)
            return MockResponse(status: 200, body: Data(), extraHeaders: ["etag": "e1"])
        }
        server.route("POST", "/v2/uploads/u1/complete") { req in
            XCTAssertTrue(String(data: req.body, encoding: .utf8)?.contains(#""etag":"e1""#) == true)
            return MockResponse(status: 200, body: Data())
        }
        let writer = try await client.newStreamUpload(remotePath: "/big.bin", totalSize: 5)
        XCTAssertEqual(try writer.partSize(), 5)
        XCTAssertEqual(try writer.totalParts(), 1)
        try await writer.writePart(partNum: 1, data: Data("hello".utf8))
        try await writer.complete(finalPartNum: 1, finalData: Data())
        XCTAssertEqual(uploaded.get(), Data("hello".utf8))

        server.route("GET", "/v1/fs/big.bin") { _ in
            MockResponse(status: 200, body: Data("download".utf8))
        }
        var downloaded = Data()
        for try await chunk in try await client.downloadStream(remotePath: "/big.bin") {
            downloaded.append(chunk)
        }
        XCTAssertEqual(downloaded, Data("download".utf8))

        server.route("GET", "/v1/vault/read") { _ in
            MockResponse(status: 200, body: Data(#"{"secrets":["s1"]}"#.utf8), contentType: "application/json")
        }
        server.route("GET", "/v1/vault/read/s1/token") { _ in
            MockResponse(status: 200, body: Data("secret-value".utf8))
        }
        let readable = try await client.vaultListReadableSecrets()
        let field = try await client.vaultReadSecretField(name: "s1", field: "token")
        XCTAssertEqual(readable, ["s1"])
        XCTAssertEqual(field, "secret-value")

        let token = Drive9CancelToken()
        token.cancel()
        do {
            try await client.downloadFile(remotePath: "/x", localPath: "/tmp/nope", cancel: token)
            XCTFail("expected cancelled")
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, _, _, _) = error else { return XCTFail("unexpected variant: \(error)") }
            XCTAssertEqual(code, "cancelled")
        }
    }

    func testPatchFilePartsValidatesInputsThenUploadsPatchParts() async throws {
        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        await assertPatchValidationError(client: client, newSize: -1, partSize: 100, dirtyParts: [1], wantToken: "new_size")
        await assertPatchValidationError(client: client, newSize: 100, partSize: 0, dirtyParts: [1], wantToken: "part_size")
        await assertPatchValidationError(client: client, newSize: 100, partSize: 100, dirtyParts: [0, 1], wantToken: "dirty_parts")

        let patched = ReceivedBody()
        server.route("PATCH", "/v1/fs/r") { req in
            XCTAssertTrue(String(data: req.body, encoding: .utf8)?.contains(#""dirty_parts""#) == true)
            let body = #"{"upload_id":"p1","part_size":4,"upload_parts":[{"number":1,"url":"\#(self.server.baseURL)/patch-part","size":4,"headers":{}}],"copied_parts":[]}"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }
        server.route("PUT", "/patch-part") { req in
            patched.set(req.body)
            XCTAssertNil(req.headers["x-amz-checksum-sha256"])
            return MockResponse(status: 200, body: Data())
        }
        server.route("POST", "/v1/uploads/p1/complete") { _ in MockResponse(status: 200, body: Data()) }

        let local = FileManager.default.temporaryDirectory
            .appendingPathComponent("drive9-swift-patch-\(UUID().uuidString).bin")
        try Data("abcdef".utf8).write(to: local)
        defer { try? FileManager.default.removeItem(at: local) }
        try await client.patchFileParts(
            localPath: local.path,
            remotePath: "/r",
            dirtyParts: [1],
            newSize: 6,
            partSize: 4
        )
        XCTAssertEqual(patched.get(), Data("abcd".utf8))
    }

    func testPatchFilePartsSendsPresignedChecksumHeader() async throws {
        let client = Drive9Client(baseUrl: server.baseURL, apiKey: "k")
        let patched = ReceivedBody()
        server.route("PATCH", "/v1/fs/r") { _ in
            let body = #"{"upload_id":"p1","part_size":8,"upload_parts":[{"number":1,"url":"\#(self.server.baseURL)/patch-part-checksum","size":8,"headers":{"x-amz-checksum-sha256":"placeholder"}}],"copied_parts":[]}"#
            return MockResponse(status: 200, body: Data(body.utf8), contentType: "application/json")
        }
        server.route("PUT", "/patch-part-checksum") { req in
            patched.set(req.body)
            XCTAssertEqual(req.headers["x-amz-checksum-sha256"], "gQ/y+yQqXe5CIPLLDmpRmJH7Z/L4KKbKtO+IlGM7H1A=")
            return MockResponse(status: 200, body: Data())
        }
        server.route("POST", "/v1/uploads/p1/complete") { _ in MockResponse(status: 200, body: Data()) }

        let local = FileManager.default.temporaryDirectory
            .appendingPathComponent("drive9-swift-patch-checksum-\(UUID().uuidString).bin")
        try Data("testdata".utf8).write(to: local)
        defer { try? FileManager.default.removeItem(at: local) }
        try await client.patchFileParts(
            localPath: local.path,
            remotePath: "/r",
            dirtyParts: [1],
            newSize: 8,
            partSize: 8
        )
        XCTAssertEqual(patched.get(), Data("testdata".utf8))
    }

    private func assertDrive9Code<T>(
        _ want: String,
        _ body: () async throws -> T,
        file: StaticString = #file,
        line: UInt = #line
    ) async {
        do {
            _ = try await body()
            XCTFail("expected \(want)", file: file, line: line)
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, _, _, _) = error else {
                XCTFail("unexpected variant: \(error)", file: file, line: line)
                return
            }
            XCTAssertEqual(code, want, file: file, line: line)
        } catch {
            XCTFail("unexpected error type: \(error)", file: file, line: line)
        }
    }

    private func assertPatchValidationError(
        client: Drive9Client,
        newSize: Int64,
        partSize: Int64,
        dirtyParts: [Int32],
        wantToken: String,
        file: StaticString = #file,
        line: UInt = #line
    ) async {
        do {
            try await client.patchFileParts(
                localPath: "/tmp/local",
                remotePath: "/r",
                dirtyParts: dirtyParts,
                newSize: newSize,
                partSize: partSize
            )
            XCTFail("expected validation error", file: file, line: line)
        } catch let error as Drive9Exception {
            guard case let .Drive9(code, _, detail, _) = error else {
                XCTFail("unexpected variant: \(error)", file: file, line: line)
                return
            }
            XCTAssertEqual(code, "other", file: file, line: line)
            XCTAssertTrue(detail.contains(wantToken), "expected \(wantToken) in: \(detail)", file: file, line: line)
        } catch {
            XCTFail("unexpected error type: \(error)", file: file, line: line)
        }
    }
}
