import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

private let defaultSmallFileThreshold: Int64 = 50_000
private let defaultPartSize: Int64 = 8 * 1024 * 1024

public final class Drive9Client: @unchecked Sendable {
    private let baseUrlValue: String
    private let apiKeyValue: String?
    private let session: URLSession
    private let smallFileThreshold: Int64

    public init(baseUrl: String, apiKey: String) {
        self.baseUrlValue = baseUrl.trimmedTrailingSlashes()
        self.apiKeyValue = apiKey.isEmpty ? nil : apiKey
        self.session = URLSession.shared
        self.smallFileThreshold = defaultSmallFileThreshold
    }

    public init(baseUrl: String, apiKey: String?, smallFileThreshold: Int64 = 50_000) {
        self.baseUrlValue = baseUrl.trimmedTrailingSlashes()
        self.apiKeyValue = apiKey?.isEmpty == true ? nil : apiKey
        self.session = URLSession.shared
        self.smallFileThreshold = smallFileThreshold
    }

    public static func defaultClient() -> Drive9Client {
        let env = ProcessInfo.processInfo.environment
        if let server = env["DRIVE9_SERVER"], !server.isEmpty {
            return Drive9Client(baseUrl: server, apiKey: env["DRIVE9_API_KEY"])
        }
        if let key = env["DRIVE9_API_KEY"], !key.isEmpty {
            return Drive9Client(baseUrl: "https://api.drive9.ai", apiKey: key)
        }
        if let cfg = loadConfigFile() {
            return Drive9Client(baseUrl: cfg.server, apiKey: cfg.apiKey)
        }
        return Drive9Client(baseUrl: "https://api.drive9.ai", apiKey: nil)
    }

    public func withSmallFileThreshold(_ threshold: Int64) -> Drive9Client {
        Drive9Client(baseUrl: baseUrlValue, apiKey: apiKeyValue, smallFileThreshold: threshold)
    }

    public func baseUrl() -> String { baseUrlValue }

    public func apiKey() -> String? { apiKeyValue }

    public func write(path: String, data: Data, expectedRevision: Int64? = nil) async throws {
        try await writeWithRevision(path: path, data: data, expectedRevision: expectedRevision ?? -1)
    }

    public func writeWithRevision(path: String, data: Data, expectedRevision: Int64) async throws {
        var request = makeRequest(method: "PUT", url: fsUrl(path))
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        if expectedRevision >= 0 {
            request.setValue(String(expectedRevision), forHTTPHeaderField: "X-Dat9-Expected-Revision")
        }
        _ = try await send(request, body: data)
    }

    public func read(path: String) async throws -> Data {
        try await send(makeRequest(method: "GET", url: fsUrl(path))).data
    }

    public func list(path: String) async throws -> [Drive9FileInfo] {
        var components = URLComponents(url: fsUrl(path), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "list", value: "1")]
        let response = try await send(makeRequest(method: "GET", url: components.url!))
        let root = try parseJson(response.data)
        guard let object = root as? [String: Any], let entries = object["entries"] as? [[String: Any]] else {
            return []
        }
        return entries.map { entry in
            Drive9FileInfo(
                name: entry["name"] as? String ?? "",
                size: int64(entry["size"]) ?? 0,
                isDir: entry["isDir"] as? Bool ?? false,
                mtimeUnix: int64(entry["mtime"])
            )
        }
    }

    public func stat(path: String) async throws -> Drive9StatResult {
        let response = try await send(makeRequest(method: "HEAD", url: fsUrl(path)))
        let headers = response.http.allHeaderFields
        return Drive9StatResult(
            size: int64(header(headers, "Content-Length")) ?? 0,
            isDir: (header(headers, "X-Dat9-IsDir") as? String) == "true",
            revision: int64(header(headers, "X-Dat9-Revision")) ?? 0,
            mtimeUnix: headerTime(header(headers, "X-Dat9-Mtime"))
        )
    }

    public func delete(path: String) async throws {
        _ = try await send(makeRequest(method: "DELETE", url: fsUrl(path)))
    }

    public func copy(srcPath: String, dstPath: String) async throws {
        var components = URLComponents(url: fsUrl(dstPath), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "copy", value: nil)]
        var request = makeRequest(method: "POST", url: components.url!)
        request.setValue(normalizedPath(srcPath), forHTTPHeaderField: "X-Dat9-Copy-Source")
        _ = try await send(request)
    }

    public func rename(oldPath: String, newPath: String) async throws {
        var components = URLComponents(url: fsUrl(newPath), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "rename", value: nil)]
        var request = makeRequest(method: "POST", url: components.url!)
        request.setValue(normalizedPath(oldPath), forHTTPHeaderField: "X-Dat9-Rename-Source")
        _ = try await send(request)
    }

    public func mkdir(path: String) async throws {
        var components = URLComponents(url: fsUrl(path), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "mkdir", value: nil)]
        _ = try await send(makeRequest(method: "POST", url: components.url!))
    }

    public func grep(query: String, pathPrefix: String, limit: Int32 = 0) async throws -> [Drive9SearchResult] {
        var components = URLComponents(url: fsUrl(pathPrefix), resolvingAgainstBaseURL: false)!
        var items = [URLQueryItem(name: "grep", value: query)]
        if limit > 0 { items.append(URLQueryItem(name: "limit", value: String(limit))) }
        components.queryItems = items
        let response = try await send(makeRequest(method: "GET", url: components.url!))
        return try parseSearchResults(response.data)
    }

    public func find(pathPrefix: String, params: [String: String] = [:]) async throws -> [Drive9SearchResult] {
        var components = URLComponents(url: fsUrl(pathPrefix), resolvingAgainstBaseURL: false)!
        var items = params.map { URLQueryItem(name: $0.key, value: $0.value) }
        items.append(URLQueryItem(name: "find", value: ""))
        components.queryItems = items
        let response = try await send(makeRequest(method: "GET", url: components.url!))
        return try parseSearchResults(response.data)
    }

    public func sql(query: String) async throws -> [String] {
        let payload = try JSONSerialization.data(withJSONObject: ["query": query], options: [])
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v1/sql")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let response = try await send(request, body: payload)
        let root = try parseJson(response.data)
        guard let rows = root as? [Any] else { return [] }
        return try rows.map { row in
            let data = try JSONSerialization.data(withJSONObject: row, options: [])
            return String(data: data, encoding: .utf8) ?? "{}"
        }
    }

    public func downloadStream(remotePath: String, cancel: Drive9CancelToken? = nil) async throws -> Drive9DownloadAsyncSequence {
        let data = try await read(path: remotePath)
        return Drive9DownloadAsyncSequence(chunks: splitChunks(data), cancel: cancel)
    }

    public func downloadRangeStream(
        remotePath: String,
        offset: Int64,
        length: Int64,
        cancel: Drive9CancelToken? = nil
    ) async throws -> Drive9DownloadAsyncSequence {
        if length <= 0 { return Drive9DownloadAsyncSequence(chunks: [], cancel: cancel) }
        var request = makeRequest(method: "GET", url: fsUrl(remotePath))
        request.setValue("bytes=\(offset)-\(offset + length - 1)", forHTTPHeaderField: "Range")
        do {
            let response = try await send(request)
            return Drive9DownloadAsyncSequence(chunks: splitChunks(response.data), cancel: cancel)
        } catch let error as Drive9Exception {
            if case let .Drive9(_, statusCode, _, _) = error, statusCode == 416 {
                return Drive9DownloadAsyncSequence(chunks: [], cancel: cancel)
            }
            throw error
        }
    }

    public func uploadStream<Source: AsyncSequence>(
        remotePath: String,
        totalSize: Int64,
        source: Source,
        expectedRevision: Int64? = nil,
        progress: Drive9ProgressListener? = nil,
        cancel: Drive9CancelToken? = nil
    ) async throws where Source.Element == Data {
        try checkCancel(cancel)
        let writer = try await newStreamUpload(remotePath: remotePath, totalSize: totalSize, expectedRevision: expectedRevision)
        var partNum: Int32 = 1
        var transferred: UInt64 = 0
        var buffer = Data()
        let partSize = Int(try writer.partSize())
        do {
            for try await chunk in source {
                try checkCancel(cancel)
                buffer.append(chunk)
                while buffer.count >= partSize {
                    let data = buffer.prefix(partSize)
                    try await writer.writePart(partNum: partNum, data: Data(data))
                    buffer.removeFirst(partSize)
                    transferred += UInt64(data.count)
                    progress?.onProgress(transferred: transferred, total: UInt64(totalSize))
                    partNum += 1
                }
            }
            try await writer.complete(finalPartNum: partNum, finalData: buffer)
            transferred += UInt64(buffer.count)
            progress?.onProgress(transferred: transferred, total: UInt64(totalSize))
        } catch {
            try? await writer.abort()
            throw error
        }
    }

    public func newStreamUpload(
        remotePath: String,
        totalSize: Int64,
        expectedRevision: Int64? = nil
    ) async throws -> Drive9StreamUpload {
        let plan = try await initiateUploadV2(path: remotePath, size: totalSize, expectedRevision: expectedRevision ?? -1)
        return Drive9StreamUpload(client: self, plan: plan)
    }

    public func uploadFile(
        localPath: String,
        remotePath: String,
        expectedRevision: Int64? = nil,
        progress: Drive9ProgressListener? = nil,
        cancel: Drive9CancelToken? = nil
    ) async throws {
        let url = URL(fileURLWithPath: localPath)
        do {
            let attrs = try FileManager.default.attributesOfItem(atPath: localPath)
            let size = int64(attrs[.size]) ?? 0
            if size < smallFileThreshold {
                try checkCancel(cancel)
                let data = try Data(contentsOf: url)
                try await writeWithRevision(path: remotePath, data: data, expectedRevision: expectedRevision ?? -1)
                progress?.onProgress(transferred: UInt64(data.count), total: UInt64(size))
                return
            }
            try await uploadSeekable(localURL: url, remotePath: remotePath, totalSize: size, expectedRevision: expectedRevision ?? -1, progress: progress, cancel: cancel)
        } catch let error as Drive9Exception {
            throw error
        } catch {
            throw Drive9Exception.Drive9(code: "io", statusCode: nil, detail: error.localizedDescription, serverRevision: nil)
        }
    }

    public func downloadFile(
        remotePath: String,
        localPath: String,
        progress: Drive9ProgressListener? = nil,
        cancel: Drive9CancelToken? = nil
    ) async throws {
        try checkCancel(cancel)
        let response = try await send(makeRequest(method: "GET", url: fsUrl(remotePath)))
        try checkCancel(cancel)
        let target = URL(fileURLWithPath: localPath)
        let temp = target.deletingLastPathComponent()
            .appendingPathComponent(".\(target.lastPathComponent).drive9-tmp-\(UUID().uuidString)")
        do {
            try response.data.write(to: temp, options: .atomic)
            progress?.onProgress(transferred: UInt64(response.data.count), total: UInt64(response.data.count))
            if FileManager.default.fileExists(atPath: target.path) {
                try FileManager.default.removeItem(at: target)
            }
            try FileManager.default.moveItem(at: temp, to: target)
        } catch {
            try? FileManager.default.removeItem(at: temp)
            if let drive9 = error as? Drive9Exception { throw drive9 }
            throw Drive9Exception.Drive9(code: "io", statusCode: nil, detail: error.localizedDescription, serverRevision: nil)
        }
    }

    public func patchFileParts(
        localPath: String,
        remotePath: String,
        dirtyParts: [Int32],
        newSize: Int64,
        partSize: Int64,
        expectedRevision: Int64? = nil
    ) async throws {
        if newSize < 0 {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "patchFileParts: new_size must be non-negative", serverRevision: nil)
        }
        if partSize <= 0 {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "patchFileParts: part_size must be positive", serverRevision: nil)
        }
        if dirtyParts.contains(where: { $0 <= 0 }) {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "patchFileParts: dirty_parts must be 1-based positive part numbers", serverRevision: nil)
        }
        var payload: [String: Any] = [
            "new_size": newSize,
            "dirty_parts": dirtyParts,
            "part_size": partSize,
        ]
        if let expectedRevision { payload["expected_revision"] = expectedRevision }
        var request = makeRequest(method: "PATCH", url: fsUrl(remotePath))
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let response = try await send(request, body: try JSONSerialization.data(withJSONObject: payload))
        let plan = try parsePatchPlan(response.data)
        let handle = try FileHandle(forReadingFrom: URL(fileURLWithPath: localPath))
        defer { try? handle.close() }
        for part in plan.uploadParts {
            let original: Data?
            if let readUrl = part.readUrl {
                original = try await rawGet(url: readUrl, headers: part.readHeaders)
            } else {
                original = nil
            }
            let data = try readLocalPart(handle: handle, number: part.number, partSize: plan.partSize, requestedSize: part.size, original: original)
            try await uploadPatchPart(part, data: data)
        }
        try await completeUpload(uploadId: plan.uploadId)
    }

    public func resumeUpload(localPath: String, remotePath: String, totalSize: Int64) async throws {
        let meta = try await queryUpload(path: remotePath)
        let handle = try FileHandle(forReadingFrom: URL(fileURLWithPath: localPath))
        defer { try? handle.close() }
        let checksums = try computePartChecksums(handle: handle, totalSize: totalSize, partSize: defaultPartSize)
        let plan = try await requestResume(uploadId: meta.uploadId, checksums: checksums)
        for part in plan.parts {
            let psize = plan.partSize > 0 ? plan.partSize : defaultPartSize
            let data = try readAt(handle: handle, offset: Int64(part.number - 1) * psize, size: Int(part.size))
            _ = try await uploadOnePart(part, data: data)
        }
        try await completeUpload(uploadId: plan.uploadId)
    }

    public func createVaultSecret(name: String, fields: [String: Any]) async throws -> Drive9VaultSecret {
        var request = makeRequest(method: "POST", url: vaultUrl("/secrets"))
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = try JSONSerialization.data(withJSONObject: ["name": name, "fields": fields, "created_by": "drive9-swift"])
        return try parseVaultSecret((try await send(request, body: body)).data)
    }

    public func updateVaultSecret(name: String, fields: [String: Any]) async throws -> Drive9VaultSecret {
        var request = makeRequest(method: "PUT", url: vaultUrl("/secrets/\(urlEncode(name))"))
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = try JSONSerialization.data(withJSONObject: ["fields": fields, "updated_by": "drive9-swift"])
        return try parseVaultSecret((try await send(request, body: body)).data)
    }

    public func deleteVaultSecret(name: String) async throws {
        _ = try await send(makeRequest(method: "DELETE", url: vaultUrl("/secrets/\(urlEncode(name))")))
    }

    public func listVaultSecrets() async throws -> [Drive9VaultSecret] {
        let data = try await send(makeRequest(method: "GET", url: vaultUrl("/secrets"))).data
        let obj = try parseJson(data) as? [String: Any]
        let entries = obj?["secrets"] as? [[String: Any]] ?? []
        return try entries.map { try parseVaultSecretObject($0) }
    }

    public func issueVaultToken(agentId: String, taskId: String, scope: [String], ttlSeconds: Int64) async throws -> Drive9VaultTokenIssueResponse {
        var request = makeRequest(method: "POST", url: vaultUrl("/tokens"))
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = try JSONSerialization.data(withJSONObject: [
            "agent_id": agentId,
            "task_id": taskId,
            "scope": scope,
            "ttl_seconds": ttlSeconds,
        ])
        let obj = try parseJson((try await send(request, body: body)).data) as? [String: Any] ?? [:]
        return Drive9VaultTokenIssueResponse(
            token: obj["token"] as? String ?? "",
            tokenId: obj["token_id"] as? String ?? "",
            expiresAt: obj["expires_at"] as? String ?? ""
        )
    }

    public func revokeVaultToken(tokenId: String) async throws {
        _ = try await send(makeRequest(method: "DELETE", url: vaultUrl("/tokens/\(urlEncode(tokenId))")))
    }

    public func queryVaultAudit(secretName: String? = nil, limit: Int32 = 0) async throws -> [Drive9VaultAuditEvent] {
        var components = URLComponents(url: vaultUrl("/audit"), resolvingAgainstBaseURL: false)!
        var items = [URLQueryItem]()
        if let secretName { items.append(URLQueryItem(name: "secret", value: secretName)) }
        if limit > 0 { items.append(URLQueryItem(name: "limit", value: String(limit))) }
        if !items.isEmpty { components.queryItems = items }
        let data = try await send(makeRequest(method: "GET", url: components.url!)).data
        let obj = try parseJson(data) as? [String: Any]
        let events = obj?["events"] as? [[String: Any]] ?? []
        return events.map { row in
            Drive9VaultAuditEvent(
                eventId: row["event_id"] as? String ?? "",
                eventType: row["event_type"] as? String ?? "",
                timestamp: row["timestamp"] as? String ?? "",
                tokenId: row["token_id"] as? String,
                agentId: row["agent_id"] as? String,
                taskId: row["task_id"] as? String,
                secretName: row["secret_name"] as? String,
                fieldName: row["field_name"] as? String,
                adapter: row["adapter"] as? String,
                detailJson: row["detail"].map { jsonString($0) }
            )
        }
    }

    public func vaultListReadableSecrets() async throws -> [String] {
        try await listReadableVaultSecrets()
    }

    public func listReadableVaultSecrets() async throws -> [String] {
        let data = try await send(makeRequest(method: "GET", url: vaultUrl("/read"))).data
        let obj = try parseJson(data) as? [String: Any]
        return obj?["secrets"] as? [String] ?? []
    }

    public func readVaultSecret(name: String) async throws -> [String: Any] {
        let response = try await send(makeRequest(method: "GET", url: vaultUrl("/read/\(urlEncode(name))")))
        return try parseJson(response.data) as? [String: Any] ?? [:]
    }

    public func vaultReadSecretField(name: String, field: String) async throws -> String {
        try await readVaultSecretField(name: name, field: field)
    }

    public func readVaultSecretField(name: String, field: String) async throws -> String {
        let data = try await send(makeRequest(method: "GET", url: vaultUrl("/read/\(urlEncode(name))/\(urlEncode(field))"))).data
        return String(data: data, encoding: .utf8) ?? ""
    }

    fileprivate func uploadStreamPart(plan: Drive9UploadPlanV2, partNumber: Int32, data: Data) async throws -> String {
        let part = try await presignOnePart(uploadId: plan.uploadId, partNumber: partNumber)
        return try await uploadOnePartV2(uploadId: plan.uploadId, part: part, data: data)
    }

    fileprivate func completeStreamUpload(uploadId: String, parts: [Drive9CompletePart]) async throws {
        try await completeUploadV2(uploadId: uploadId, parts: parts)
    }

    fileprivate func abortStreamUpload(uploadId: String) async throws {
        try await abortUploadV2(uploadId: uploadId)
    }

    private func uploadSeekable(
        localURL: URL,
        remotePath: String,
        totalSize: Int64,
        expectedRevision: Int64,
        progress: Drive9ProgressListener?,
        cancel: Drive9CancelToken?
    ) async throws {
        do {
            try await uploadSeekableV2(localURL: localURL, remotePath: remotePath, totalSize: totalSize, expectedRevision: expectedRevision, progress: progress, cancel: cancel)
        } catch let error as Drive9Exception {
            if case let .Drive9(_, _, detail, _) = error, detail.contains("v2 upload API not available") {
                try await uploadSeekableV1(localURL: localURL, remotePath: remotePath, totalSize: totalSize, expectedRevision: expectedRevision, progress: progress, cancel: cancel)
                return
            }
            throw error
        }
    }

    private func uploadSeekableV1(
        localURL: URL,
        remotePath: String,
        totalSize: Int64,
        expectedRevision: Int64,
        progress: Drive9ProgressListener?,
        cancel: Drive9CancelToken?
    ) async throws {
        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }
        let checksums = try computePartChecksums(handle: handle, totalSize: totalSize, partSize: defaultPartSize)
        let plan = try await initiateUpload(path: remotePath, size: totalSize, checksums: checksums, expectedRevision: expectedRevision)
        var transferred: UInt64 = 0
        for part in plan.parts {
            try checkCancel(cancel)
            let psize = plan.partSize > 0 ? plan.partSize : defaultPartSize
            let data = try readAt(handle: handle, offset: Int64(part.number - 1) * psize, size: Int(part.size))
            _ = try await uploadOnePart(part, data: data)
            transferred += UInt64(data.count)
            progress?.onProgress(transferred: transferred, total: UInt64(totalSize))
        }
        try await completeUpload(uploadId: plan.uploadId)
    }

    private func uploadSeekableV2(
        localURL: URL,
        remotePath: String,
        totalSize: Int64,
        expectedRevision: Int64,
        progress: Drive9ProgressListener?,
        cancel: Drive9CancelToken?
    ) async throws {
        let plan = try await initiateUploadV2(path: remotePath, size: totalSize, expectedRevision: expectedRevision)
        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }
        var completed = [Drive9CompletePart]()
        var transferred: UInt64 = 0
        do {
            for partNumber in 1...plan.totalParts {
                try checkCancel(cancel)
                let offset = Int64(partNumber - 1) * plan.partSize
                let size = Int(min(plan.partSize, totalSize - offset))
                let data = try readAt(handle: handle, offset: offset, size: size)
                let presigned = try await presignOnePart(uploadId: plan.uploadId, partNumber: partNumber)
                let etag = try await uploadOnePartV2(uploadId: plan.uploadId, part: presigned, data: data)
                completed.append(Drive9CompletePart(number: partNumber, etag: etag))
                transferred += UInt64(data.count)
                progress?.onProgress(transferred: transferred, total: UInt64(totalSize))
            }
            try await completeUploadV2(uploadId: plan.uploadId, parts: completed)
        } catch {
            try? await abortUploadV2(uploadId: plan.uploadId)
            throw error
        }
    }

    private func initiateUpload(path: String, size: Int64, checksums: [String], expectedRevision: Int64) async throws -> Drive9UploadPlan {
        var payload: [String: Any] = ["path": path, "total_size": size, "part_checksums": checksums]
        if expectedRevision >= 0 { payload["expected_revision"] = expectedRevision }
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v1/uploads/initiate")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        do {
            let response = try await send(request, body: try JSONSerialization.data(withJSONObject: payload), accepted: 202...202)
            return try parseUploadPlan(response.data)
        } catch let error as Drive9Exception {
            if case let .Drive9(_, statusCode, detail, _) = error,
               statusCode == 404 || statusCode == 405 || (statusCode == 400 && detail.lowercased().contains("unknown upload action")) {
                return try await initiateUploadLegacy(path: path, size: size, checksums: checksums, expectedRevision: expectedRevision)
            }
            throw error
        }
    }

    private func initiateUploadLegacy(path: String, size: Int64, checksums: [String], expectedRevision: Int64) async throws -> Drive9UploadPlan {
        var request = makeRequest(method: "PUT", url: fsUrl(path))
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        request.setValue(String(size), forHTTPHeaderField: "X-Dat9-Content-Length")
        if !checksums.isEmpty { request.setValue(checksums.joined(separator: ","), forHTTPHeaderField: "X-Dat9-Part-Checksums") }
        if expectedRevision >= 0 { request.setValue(String(expectedRevision), forHTTPHeaderField: "X-Dat9-Expected-Revision") }
        return try parseUploadPlan((try await send(request)).data)
    }

    private func initiateUploadV2(path: String, size: Int64, expectedRevision: Int64) async throws -> Drive9UploadPlanV2 {
        var payload: [String: Any] = ["path": path, "total_size": size]
        if expectedRevision >= 0 { payload["expected_revision"] = expectedRevision }
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v2/uploads/initiate")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        do {
            return try parseUploadPlanV2((try await send(request, body: try JSONSerialization.data(withJSONObject: payload))).data)
        } catch let error as Drive9Exception {
            if case let .Drive9(_, statusCode, _, _) = error, statusCode == 404 {
                throw Drive9Exception.Drive9(code: "other", statusCode: 404, detail: "v2 upload API not available", serverRevision: nil)
            }
            throw error
        }
    }

    private func presignOnePart(uploadId: String, partNumber: Int32) async throws -> Drive9PresignedPart {
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v2/uploads/\(uploadId)/presign")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = try JSONSerialization.data(withJSONObject: ["part_number": partNumber])
        return try parsePresignedPart((try await send(request, body: body)).data)
    }

    private func uploadOnePartV2(uploadId: String, part: Drive9PresignedPart, data: Data) async throws -> String {
        let first = try await rawPut(url: part.url, headers: part.headers, data: data)
        if first.status == 403 {
            let fresh = try await presignOnePart(uploadId: uploadId, partNumber: part.number)
            let retry = try await rawPut(url: fresh.url, headers: fresh.headers, data: data)
            guard (200...299).contains(retry.status) else { throw errorFrom(data: retry.body, status: retry.status) }
            return retry.headers["etag"] ?? ""
        }
        guard (200...299).contains(first.status) else { throw errorFrom(data: first.body, status: first.status) }
        return first.headers["etag"] ?? ""
    }

    private func uploadOnePart(_ part: Drive9PartURL, data: Data) async throws -> String {
        var headers = part.headers
        headers["x-amz-checksum-crc32c"] = part.checksumCrc32c ?? crc32cBase64(data)
        let response = try await rawPut(url: part.url, headers: headers, data: data)
        guard (200...299).contains(response.status) else { throw errorFrom(data: response.body, status: response.status) }
        return response.headers["etag"] ?? ""
    }

    private func uploadPatchPart(_ part: Drive9PatchPartURL, data: Data) async throws {
        var headers = part.headers
        headers["x-amz-checksum-sha256"] = sha256Base64(data)
        let response = try await rawPut(url: part.url, headers: headers, data: data)
        guard (200...299).contains(response.status) else { throw errorFrom(data: response.body, status: response.status) }
    }

    private func completeUpload(uploadId: String) async throws {
        _ = try await send(makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v1/uploads/\(uploadId)/complete")!))
    }

    private func completeUploadV2(uploadId: String, parts: [Drive9CompletePart]) async throws {
        let body = try JSONSerialization.data(withJSONObject: ["parts": parts.map { ["number": $0.number, "etag": $0.etag] }])
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v2/uploads/\(uploadId)/complete")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        _ = try await send(request, body: body)
    }

    private func abortUploadV2(uploadId: String) async throws {
        _ = try await send(makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v2/uploads/\(uploadId)/abort")!))
    }

    private func queryUpload(path: String) async throws -> Drive9UploadMeta {
        let url = URL(string: "\(baseUrlValue)/v1/uploads?path=\(urlEncode(path))&status=UPLOADING")!
        let data = try await send(makeRequest(method: "GET", url: url)).data
        let obj = try parseJson(data) as? [String: Any]
        let uploads = obj?["uploads"] as? [[String: Any]] ?? []
        guard let first = uploads.first else {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "no active upload for \(path)", serverRevision: nil)
        }
        return Drive9UploadMeta(
            uploadId: first["upload_id"] as? String ?? "",
            partsTotal: Int32(int64(first["parts_total"]) ?? 0),
            status: first["status"] as? String ?? "",
            expiresAt: first["expires_at"] as? String ?? ""
        )
    }

    private func requestResume(uploadId: String, checksums: [String]) async throws -> Drive9UploadPlan {
        var request = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v1/uploads/\(uploadId)/resume")!)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        do {
            let data = try JSONSerialization.data(withJSONObject: ["part_checksums": checksums])
            return try parseUploadPlan((try await send(request, body: data)).data)
        } catch let error as Drive9Exception {
            if case let .Drive9(_, statusCode, detail, _) = error,
               statusCode == 400 && detail.lowercased().contains("missing x-dat9-part-checksums header") {
                var legacy = makeRequest(method: "POST", url: URL(string: "\(baseUrlValue)/v1/uploads/\(uploadId)/resume")!)
                if !checksums.isEmpty { legacy.setValue(checksums.joined(separator: ","), forHTTPHeaderField: "X-Dat9-Part-Checksums") }
                return try parseUploadPlan((try await send(legacy)).data)
            }
            throw error
        }
    }

    private func makeRequest(method: String, url: URL) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        if let apiKeyValue {
            request.setValue("Bearer \(apiKeyValue)", forHTTPHeaderField: "Authorization")
        }
        request.setValue("drive9-swift-native/0.2", forHTTPHeaderField: "User-Agent")
        return request
    }

    private func send(_ request: URLRequest, body: Data? = nil, accepted: ClosedRange<Int> = 200...299) async throws -> (data: Data, http: HTTPURLResponse) {
        do {
            let result: (Data, URLResponse)
            if let body {
                result = try await session.upload(for: request, from: body)
            } else {
                result = try await session.data(for: request)
            }
            guard let http = result.1 as? HTTPURLResponse else {
                throw Drive9Exception.Drive9(code: "request", statusCode: nil, detail: "non-HTTP response", serverRevision: nil)
            }
            guard accepted.contains(http.statusCode) else {
                throw errorFrom(data: result.0, status: http.statusCode)
            }
            return (result.0, http)
        } catch let error as Drive9Exception {
            throw error
        } catch {
            throw Drive9Exception.Drive9(code: "request", statusCode: nil, detail: error.localizedDescription, serverRevision: nil)
        }
    }

    private func rawGet(url: String, headers: [String: String]) async throws -> Data {
        var request = URLRequest(url: URL(string: url)!)
        request.httpMethod = "GET"
        for (k, v) in headers where k.lowercased() != "host" {
            request.setValue(v, forHTTPHeaderField: k)
        }
        return try await send(request).data
    }

    private func rawPut(url: String, headers: [String: String], data: Data) async throws -> RawResponse {
        var request = URLRequest(url: URL(string: url)!)
        request.httpMethod = "PUT"
        for (k, v) in headers where k.lowercased() != "host" {
            request.setValue(v, forHTTPHeaderField: k)
        }
        let result = try await session.upload(for: request, from: data)
        guard let http = result.1 as? HTTPURLResponse else {
            throw Drive9Exception.Drive9(code: "request", statusCode: nil, detail: "non-HTTP response", serverRevision: nil)
        }
        let headers = http.allHeaderFields.reduce(into: [String: String]()) { out, entry in
            out[String(describing: entry.key).lowercased()] = String(describing: entry.value)
        }
        return RawResponse(status: http.statusCode, body: result.0, headers: headers)
    }

    private func errorFrom(data: Data, status: Int) -> Drive9Exception {
        let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        let text = String(data: data, encoding: .utf8) ?? ""
        let detail = object?["error"] as? String ?? object?["message"] as? String ?? (text.isEmpty ? "HTTP \(status)" : text)
        let serverRevision = int64(object?["server_revision"])
        return .Drive9(
            code: status == 409 ? "conflict" : "http_status",
            statusCode: Int32(status),
            detail: detail,
            serverRevision: serverRevision
        )
    }

    private func parseJson(_ data: Data) throws -> Any {
        do {
            return try JSONSerialization.jsonObject(with: data)
        } catch {
            throw Drive9Exception.Drive9(code: "json", statusCode: nil, detail: error.localizedDescription, serverRevision: nil)
        }
    }

    private func parseSearchResults(_ data: Data) throws -> [Drive9SearchResult] {
        let root = try parseJson(data)
        guard let rows = root as? [[String: Any]] else { return [] }
        return rows.map { row in
            Drive9SearchResult(
                path: row["path"] as? String ?? "",
                name: row["name"] as? String ?? "",
                sizeBytes: int64(row["size_bytes"]) ?? 0,
                score: row["score"] as? Double
            )
        }
    }

    private func parseUploadPlan(_ data: Data) throws -> Drive9UploadPlan {
        let obj = try parseJson(data) as? [String: Any] ?? [:]
        let parts = (obj["parts"] as? [[String: Any]] ?? []).map(parsePartURL)
        return Drive9UploadPlan(uploadId: obj["upload_id"] as? String ?? "", partSize: int64(obj["part_size"]) ?? 0, parts: parts)
    }

    private func parseUploadPlanV2(_ data: Data) throws -> Drive9UploadPlanV2 {
        let obj = try parseJson(data) as? [String: Any] ?? [:]
        return Drive9UploadPlanV2(
            uploadId: obj["upload_id"] as? String ?? "",
            key: obj["key"] as? String ?? "",
            partSize: int64(obj["part_size"]) ?? defaultPartSize,
            totalParts: Int32(int64(obj["total_parts"]) ?? 0)
        )
    }

    private func parsePresignedPart(_ data: Data) throws -> Drive9PresignedPart {
        let obj = try parseJson(data) as? [String: Any] ?? [:]
        return Drive9PresignedPart(
            number: Int32(int64(obj["number"]) ?? 0),
            url: obj["url"] as? String ?? "",
            size: int64(obj["size"]) ?? 0,
            headers: stringMap(obj["headers"])
        )
    }

    private func parsePatchPlan(_ data: Data) throws -> Drive9PatchPlan {
        let obj = try parseJson(data) as? [String: Any] ?? [:]
        return Drive9PatchPlan(
            uploadId: obj["upload_id"] as? String ?? "",
            partSize: int64(obj["part_size"]) ?? defaultPartSize,
            uploadParts: (obj["upload_parts"] as? [[String: Any]] ?? []).map(parsePatchPartURL),
            copiedParts: (obj["copied_parts"] as? [Any] ?? []).compactMap { int64($0).map(Int32.init) }
        )
    }

    private func parseVaultSecret(_ data: Data) throws -> Drive9VaultSecret {
        try parseVaultSecretObject(parseJson(data) as? [String: Any] ?? [:])
    }

    private func parseVaultSecretObject(_ obj: [String: Any]) throws -> Drive9VaultSecret {
        Drive9VaultSecret(
            name: obj["name"] as? String ?? "",
            secretType: obj["secret_type"] as? String ?? "",
            revision: int64(obj["revision"]) ?? 0,
            createdBy: obj["created_by"] as? String ?? "",
            createdAt: obj["created_at"] as? String ?? "",
            updatedAt: obj["updated_at"] as? String ?? ""
        )
    }

    private func fsUrl(_ path: String) -> URL {
        URL(string: "\(baseUrlValue)/v1/fs\(encodedPath(path))")!
    }

    private func vaultUrl(_ path: String) -> URL {
        URL(string: "\(baseUrlValue)/v1/vault\(path.hasPrefix("/") ? path : "/\(path)")")!
    }

    private func normalizedPath(_ path: String) -> String {
        path.hasPrefix("/") ? path : "/\(path)"
    }

    private func encodedPath(_ path: String) -> String {
        normalizedPath(path).split(separator: "/", omittingEmptySubsequences: false)
            .map { String($0).addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? String($0) }
            .joined(separator: "/")
    }
}

public final class Drive9StreamUpload: @unchecked Sendable {
    private let client: Drive9Client
    private let plan: Drive9UploadPlanV2
    private var uploaded: [Int32: Drive9CompletePart] = [:]
    private var completed = false
    private var aborted = false

    fileprivate init(client: Drive9Client, plan: Drive9UploadPlanV2) {
        self.client = client
        self.plan = plan
    }

    public func writePart(partNum: Int32, data: Data) throws {
        try runBlocking { try await self.writePartAsync(partNum: partNum, data: data) }
    }

    public func writePart(partNum: Int32, data: Data) async throws {
        try await writePartAsync(partNum: partNum, data: data)
    }

    private func writePartAsync(partNum: Int32, data: Data) async throws {
        try ensureOpen()
        if partNum < 1 {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "part number must be >= 1", serverRevision: nil)
        }
        if partNum > plan.totalParts {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "part number \(partNum) exceeds total_parts \(plan.totalParts)", serverRevision: nil)
        }
        if uploaded[partNum] != nil {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "part \(partNum) already uploaded", serverRevision: nil)
        }
        let etag = try await client.uploadStreamPart(plan: plan, partNumber: partNum, data: data)
        uploaded[partNum] = Drive9CompletePart(number: partNum, etag: etag)
    }

    public func complete(finalPartNum: Int32, finalData: Data) throws {
        try runBlocking { try await self.completeAsync(finalPartNum: finalPartNum, finalData: finalData) }
    }

    public func complete(finalPartNum: Int32, finalData: Data) async throws {
        try await completeAsync(finalPartNum: finalPartNum, finalData: finalData)
    }

    private func completeAsync(finalPartNum: Int32, finalData: Data) async throws {
        try ensureOpen()
        if !finalData.isEmpty {
            try await writePartAsync(partNum: finalPartNum, data: finalData)
        }
        let snapshot = uploaded
        guard let maxPart = snapshot.keys.max() else {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "no parts uploaded in stream upload", serverRevision: nil)
        }
        let parts = try (1...maxPart).map { part -> Drive9CompletePart in
            guard let cp = snapshot[part] else {
                throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "missing part \(part) in stream upload", serverRevision: nil)
            }
            return cp
        }
        completed = true
        try await client.completeStreamUpload(uploadId: plan.uploadId, parts: parts)
    }

    public func abort() throws {
        try runBlocking { try await self.abortAsync() }
    }

    public func abort() async throws {
        try await abortAsync()
    }

    private func abortAsync() async throws {
        if aborted {
            return
        }
        aborted = true
        let shouldAbort = !completed
        if shouldAbort { try await client.abortStreamUpload(uploadId: plan.uploadId) }
    }

    public func partSize() throws -> Int64 { plan.partSize }

    public func totalParts() throws -> Int32 { plan.totalParts }

    private func ensureOpen() throws {
        let isCompleted = completed
        let isAborted = aborted
        if isCompleted {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "stream writer already completed", serverRevision: nil)
        }
        if isAborted {
            throw Drive9Exception.Drive9(code: "other", statusCode: nil, detail: "stream writer already aborted", serverRevision: nil)
        }
    }
}

public final class Drive9StreamDownload: @unchecked Sendable {
    private var chunks: [Data]
    private let lock = NSLock()

    fileprivate init(chunks: [Data]) {
        self.chunks = chunks
    }

    public func readChunk() throws -> Data? {
        lock.lock()
        defer { lock.unlock() }
        if chunks.isEmpty { return nil }
        return chunks.removeFirst()
    }
    public func closeStream() {}
}

public struct Drive9DownloadAsyncSequence: AsyncSequence, Sendable {
    public typealias Element = Data
    private let chunks: [Data]
    private let cancel: Drive9CancelToken?

    fileprivate init(chunks: [Data], cancel: Drive9CancelToken?) {
        self.chunks = chunks
        self.cancel = cancel
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var chunks: [Data]
        private let cancel: Drive9CancelToken?

        fileprivate init(chunks: [Data], cancel: Drive9CancelToken?) {
            self.chunks = chunks
            self.cancel = cancel
        }

        public mutating func next() async throws -> Data? {
            try checkCancel(cancel)
            if chunks.isEmpty { return nil }
            return chunks.removeFirst()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(chunks: chunks, cancel: cancel) }
}

public final class Drive9CancelToken: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false

    public init() {}
    public func cancel() { lock.lock(); cancelled = true; lock.unlock() }
    public func isCancelled() -> Bool { lock.lock(); defer { lock.unlock() }; return cancelled }
}

public protocol Drive9ProgressListener: AnyObject, Sendable {
    func onProgress(transferred: UInt64, total: UInt64)
}

public struct Drive9FileInfo: Equatable, Hashable, Sendable {
    public var name: String
    public var size: Int64
    public var isDir: Bool
    public var mtimeUnix: Int64?

    public init(name: String, size: Int64, isDir: Bool, mtimeUnix: Int64?) {
        self.name = name
        self.size = size
        self.isDir = isDir
        self.mtimeUnix = mtimeUnix
    }
}

public struct Drive9SearchResult: Equatable, Hashable, Sendable {
    public var path: String
    public var name: String
    public var sizeBytes: Int64
    public var score: Double?

    public init(path: String, name: String, sizeBytes: Int64, score: Double?) {
        self.path = path
        self.name = name
        self.sizeBytes = sizeBytes
        self.score = score
    }
}

public struct Drive9StatResult: Equatable, Hashable, Sendable {
    public var size: Int64
    public var isDir: Bool
    public var revision: Int64
    public var mtimeUnix: Int64?

    public init(size: Int64, isDir: Bool, revision: Int64, mtimeUnix: Int64?) {
        self.size = size
        self.isDir = isDir
        self.revision = revision
        self.mtimeUnix = mtimeUnix
    }
}

public struct Drive9PartURL: Equatable, Hashable, Sendable {
    public var number: Int32
    public var url: String
    public var size: Int64
    public var checksumSha256: String?
    public var checksumCrc32c: String?
    public var headers: [String: String]
    public var expiresAt: String?
}

public struct Drive9UploadPlan: Equatable, Hashable, Sendable {
    public var uploadId: String
    public var partSize: Int64
    public var parts: [Drive9PartURL]
}

public struct Drive9PatchPartURL: Equatable, Hashable, Sendable {
    public var number: Int32
    public var url: String
    public var size: Int64
    public var headers: [String: String]
    public var expiresAt: String?
    public var readUrl: String?
    public var readHeaders: [String: String]
}

public struct Drive9PatchPlan: Equatable, Hashable, Sendable {
    public var uploadId: String
    public var partSize: Int64
    public var uploadParts: [Drive9PatchPartURL]
    public var copiedParts: [Int32]
}

public struct Drive9UploadMeta: Equatable, Hashable, Sendable {
    public var uploadId: String
    public var partsTotal: Int32
    public var status: String
    public var expiresAt: String
}

public struct Drive9VaultSecret: Equatable, Hashable, Sendable {
    public var name: String
    public var secretType: String
    public var revision: Int64
    public var createdBy: String
    public var createdAt: String
    public var updatedAt: String
}

public struct Drive9VaultTokenIssueResponse: Equatable, Hashable, Sendable {
    public var token: String
    public var tokenId: String
    public var expiresAt: String
}

public struct Drive9VaultAuditEvent: Equatable, Hashable, Sendable {
    public var eventId: String
    public var eventType: String
    public var timestamp: String
    public var tokenId: String?
    public var agentId: String?
    public var taskId: String?
    public var secretName: String?
    public var fieldName: String?
    public var adapter: String?
    public var detailJson: String?
}

fileprivate struct Drive9UploadPlanV2: Equatable, Hashable, Sendable {
    var uploadId: String
    var key: String
    var partSize: Int64
    var totalParts: Int32
}

fileprivate struct Drive9PresignedPart: Equatable, Hashable, Sendable {
    var number: Int32
    var url: String
    var size: Int64
    var headers: [String: String]
}

fileprivate struct Drive9CompletePart: Equatable, Hashable, Sendable {
    var number: Int32
    var etag: String
}

fileprivate struct RawResponse {
    var status: Int
    var body: Data
    var headers: [String: String]
}

public enum Drive9Exception: Error, Equatable, Hashable, LocalizedError, Sendable {
    case Drive9(code: String, statusCode: Int32?, detail: String, serverRevision: Int64?)

    public var errorDescription: String? {
        switch self {
        case let .Drive9(code, statusCode, detail, serverRevision):
            return "Drive9(code: \(code), statusCode: \(String(describing: statusCode)), detail: \(detail), serverRevision: \(String(describing: serverRevision)))"
        }
    }
}

private func parsePartURL(_ obj: [String: Any]) -> Drive9PartURL {
    Drive9PartURL(
        number: Int32(int64(obj["number"]) ?? 0),
        url: obj["url"] as? String ?? "",
        size: int64(obj["size"]) ?? 0,
        checksumSha256: obj["checksum_sha256"] as? String,
        checksumCrc32c: obj["checksum_crc32c"] as? String,
        headers: stringMap(obj["headers"]),
        expiresAt: obj["expires_at"] as? String
    )
}

private func parsePatchPartURL(_ obj: [String: Any]) -> Drive9PatchPartURL {
    Drive9PatchPartURL(
        number: Int32(int64(obj["number"]) ?? 0),
        url: obj["url"] as? String ?? "",
        size: int64(obj["size"]) ?? 0,
        headers: stringMap(obj["headers"]),
        expiresAt: obj["expires_at"] as? String,
        readUrl: obj["read_url"] as? String,
        readHeaders: stringMap(obj["read_headers"])
    )
}

private func int64(_ value: Any?) -> Int64? {
    switch value {
    case let number as NSNumber:
        return number.int64Value
    case let string as String:
        return Int64(string)
    case let int as Int:
        return Int64(int)
    case let int32 as Int32:
        return Int64(int32)
    case let int64 as Int64:
        return int64
    default:
        return nil
    }
}

private func headerTime(_ value: Any?) -> Int64? {
    if let raw = int64(value) { return raw }
    guard let string = value as? String else { return nil }
    return ISO8601DateFormatter().date(from: string).map { Int64($0.timeIntervalSince1970) }
}

private func header(_ headers: [AnyHashable: Any], _ name: String) -> Any? {
    headers.first { element in
        String(describing: element.key).lowercased() == name.lowercased()
    }?.value
}

private func stringMap(_ value: Any?) -> [String: String] {
    guard let map = value as? [String: Any] else { return [:] }
    return map.reduce(into: [String: String]()) { out, item in
        out[item.key] = String(describing: item.value)
    }
}

private func jsonString(_ value: Any) -> String {
    guard JSONSerialization.isValidJSONObject(value),
          let data = try? JSONSerialization.data(withJSONObject: value),
          let string = String(data: data, encoding: .utf8) else {
        return String(describing: value)
    }
    return string
}

private func urlEncode(_ value: String) -> String {
    value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? value
}

private func checkCancel(_ token: Drive9CancelToken?) throws {
    if token?.isCancelled() == true {
        throw Drive9Exception.Drive9(code: "cancelled", statusCode: nil, detail: "operation cancelled", serverRevision: nil)
    }
}

private func runBlocking<T>(_ body: @escaping () async throws -> T) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    var result: Result<T, Error>?
    Task {
        do {
            result = .success(try await body())
        } catch {
            result = .failure(error)
        }
        semaphore.signal()
    }
    semaphore.wait()
    return try result!.get()
}

private func splitChunks(_ data: Data, size: Int = 64 * 1024) -> [Data] {
    if data.isEmpty { return [] }
    var out = [Data]()
    var offset = 0
    while offset < data.count {
        let end = min(offset + size, data.count)
        out.append(data.subdata(in: offset..<end))
        offset = end
    }
    return out
}

private func readAt(handle: FileHandle, offset: Int64, size: Int) throws -> Data {
    if size <= 0 { return Data() }
    try handle.seek(toOffset: UInt64(offset))
    return handle.readData(ofLength: size)
}

private func readLocalPart(handle: FileHandle, number: Int32, partSize: Int64, requestedSize: Int64, original: Data?) throws -> Data {
    let data = try readAt(handle: handle, offset: Int64(number - 1) * partSize, size: Int(requestedSize))
    return data.isEmpty ? (original ?? data) : data
}

private func computePartChecksums(handle: FileHandle, totalSize: Int64, partSize: Int64) throws -> [String] {
    var out = [String]()
    var offset: Int64 = 0
    while offset < totalSize {
        let size = min(partSize, totalSize - offset)
        out.append(crc32cBase64(try readAt(handle: handle, offset: offset, size: Int(size))))
        offset += size
    }
    return out
}

private func crc32cBase64(_ data: Data) -> String {
    var crc: UInt32 = 0xffffffff
    for byte in data {
        crc ^= UInt32(byte)
        for _ in 0..<8 {
            let mask = UInt32(bitPattern: -Int32(crc & 1))
            crc = (crc >> 1) ^ (0x82f63b78 & mask)
        }
    }
    crc ^= 0xffffffff
    let bytes = [
        UInt8((crc >> 24) & 0xff),
        UInt8((crc >> 16) & 0xff),
        UInt8((crc >> 8) & 0xff),
        UInt8(crc & 0xff),
    ]
    return Data(bytes).base64EncodedString()
}

private func sha256Base64(_ data: Data) -> String {
    let hash = sha256(data)
    return Data(hash).base64EncodedString()
}

private func sha256(_ data: Data) -> [UInt8] {
    let k: [UInt32] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ]
    var message = [UInt8](data)
    let bitLength = UInt64(message.count) * 8
    message.append(0x80)
    while message.count % 64 != 56 {
        message.append(0)
    }
    for shift in stride(from: 56, through: 0, by: -8) {
        message.append(UInt8((bitLength >> UInt64(shift)) & 0xff))
    }

    var h: [UInt32] = [
        0x6a09e667,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ]

    for chunkStart in stride(from: 0, to: message.count, by: 64) {
        var w = [UInt32](repeating: 0, count: 64)
        for i in 0..<16 {
            let j = chunkStart + i * 4
            w[i] = (UInt32(message[j]) << 24)
                | (UInt32(message[j + 1]) << 16)
                | (UInt32(message[j + 2]) << 8)
                | UInt32(message[j + 3])
        }
        for i in 16..<64 {
            let s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3)
            let s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10)
            w[i] = w[i - 16] &+ s0 &+ w[i - 7] &+ s1
        }

        var a = h[0]
        var b = h[1]
        var c = h[2]
        var d = h[3]
        var e = h[4]
        var f = h[5]
        var g = h[6]
        var hh = h[7]

        for i in 0..<64 {
            let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
            let ch = (e & f) ^ ((~e) & g)
            let temp1 = hh &+ s1 &+ ch &+ k[i] &+ w[i]
            let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
            let maj = (a & b) ^ (a & c) ^ (b & c)
            let temp2 = s0 &+ maj
            hh = g
            g = f
            f = e
            e = d &+ temp1
            d = c
            c = b
            b = a
            a = temp1 &+ temp2
        }

        h[0] = h[0] &+ a
        h[1] = h[1] &+ b
        h[2] = h[2] &+ c
        h[3] = h[3] &+ d
        h[4] = h[4] &+ e
        h[5] = h[5] &+ f
        h[6] = h[6] &+ g
        h[7] = h[7] &+ hh
    }

    return h.flatMap { word in
        [
            UInt8((word >> 24) & 0xff),
            UInt8((word >> 16) & 0xff),
            UInt8((word >> 8) & 0xff),
            UInt8(word & 0xff),
        ]
    }
}

private func rotr(_ value: UInt32, _ bits: UInt32) -> UInt32 {
    (value >> bits) | (value << (32 - bits))
}

private func loadConfigFile() -> (server: String, apiKey: String?)? {
    let env = ProcessInfo.processInfo.environment
    guard let home = env["HOME"] ?? env["USERPROFILE"] else { return nil }
    let path = URL(fileURLWithPath: home).appendingPathComponent(".drive9/config")
    guard let data = try? Data(contentsOf: path),
          let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else { return nil }
    let server = root["server"] as? String ?? "https://api.drive9.ai"
    let current = root["current_context"] as? String
    let contexts = root["contexts"] as? [String: Any]
    let ctx = current.flatMap { contexts?[$0] as? [String: Any] }
    return (server, ctx?["api_key"] as? String)
}

private extension String {
    func trimmedTrailingSlashes() -> String {
        var value = self
        while value.hasSuffix("/") {
            value.removeLast()
        }
        return value
    }
}
