package com.drive9.mobile

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.net.Socket
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C
import javax.net.ssl.SSLSocketFactory

private const val DEFAULT_SMALL_FILE_THRESHOLD = 50_000L
private const val DEFAULT_PART_SIZE = 8L * 1024L * 1024L

/** Idiomatic Kotlin Drive9 client implemented with platform-native HTTP. */
public class Drive9Client(
    baseUrl: String,
    apiKey: String,
    private val smallFileThreshold: Long = DEFAULT_SMALL_FILE_THRESHOLD,
) {
    private val baseUrlValue: String = baseUrl.trimEnd('/')
    private val apiKeyValue: String = apiKey
    private val json = Json { ignoreUnknownKeys = true }

    public companion object {
        public fun defaultClient(): Drive9Client {
            val envServer = System.getenv("DRIVE9_SERVER").orEmpty()
            val envKey = System.getenv("DRIVE9_API_KEY").orEmpty()
            if (envServer.isNotEmpty() || envKey.isNotEmpty()) {
                return Drive9Client(envServer.ifEmpty { "https://api.drive9.ai" }, envKey)
            }
            val cfg = loadConfigFile()
            return Drive9Client(cfg?.first ?: "https://api.drive9.ai", cfg?.second.orEmpty())
        }

        private fun loadConfigFile(): Pair<String, String?>? {
            val home = System.getenv("HOME") ?: System.getenv("USERPROFILE") ?: return null
            val file = File(File(home, ".drive9"), "config")
            val root = runCatching { Json.parseToJsonElement(file.readText()).jsonObject }.getOrNull() ?: return null
            val server = root["server"]?.jsonPrimitive?.contentOrNull ?: "https://api.drive9.ai"
            val current = root["current_context"]?.jsonPrimitive?.contentOrNull
            val key = current
                ?.let { root["contexts"]?.jsonObject?.get(it)?.jsonObject }
                ?.get("api_key")
                ?.jsonPrimitive
                ?.contentOrNull
            return server to key
        }
    }

    public fun withSmallFileThreshold(threshold: Long): Drive9Client =
        Drive9Client(baseUrlValue, apiKeyValue, threshold)

    public fun baseUrl(): String = baseUrlValue

    public fun apiKey(): String? = apiKeyValue.takeIf { it.isNotEmpty() }

    public suspend fun write(path: String, data: ByteArray): Unit =
        writeWithRevision(path, data, -1)

    public suspend fun write(
        path: String,
        data: ByteArray,
        expectedRevision: Long? = null,
    ): Unit = writeWithRevision(path, data, expectedRevision ?: -1)

    public suspend fun writeWithRevision(
        path: String,
        data: ByteArray,
        expectedRevision: Long,
    ): Unit = withContext(Dispatchers.IO) {
        request("PUT", fsUrl(path)) {
            setRequestProperty("Content-Type", "application/octet-stream")
            if (expectedRevision >= 0) setRequestProperty("X-Dat9-Expected-Revision", expectedRevision.toString())
            doOutput = true
            outputStream.use { it.write(data) }
        }.close()
    }

    public suspend fun read(path: String): ByteArray = withContext(Dispatchers.IO) {
        request("GET", fsUrl(path)).use { it.body }
    }

    public suspend fun list(path: String): List<Drive9FileInfo> = withContext(Dispatchers.IO) {
        val body = request("GET", "${fsUrl(path)}?list=1").use { it.text() }
        val entries = json.parseToJsonElement(body).jsonObject["entries"] as? JsonArray ?: JsonArray(emptyList())
        entries.map { it.jsonObject.toFileInfo() }
    }

    public suspend fun stat(path: String): Drive9StatResult = withContext(Dispatchers.IO) {
        val conn = open("HEAD", fsUrl(path))
        try {
            val status = conn.responseCode
            if (status == 404) throw Drive9Exception.Drive9("other", status, "not found: $path", null)
            if (status !in 200..299) throw errorFrom(conn, status)
            Drive9StatResult(
                size = conn.headerFieldLong("Content-Length") ?: 0L,
                isDir = conn.getHeaderField("X-Dat9-IsDir") == "true",
                revision = conn.headerFieldLong("X-Dat9-Revision") ?: 0L,
                mtimeUnix = conn.headerFieldUnixTime("X-Dat9-Mtime"),
            )
        } finally {
            conn.disconnect()
        }
    }

    public suspend fun delete(path: String): Unit = withContext(Dispatchers.IO) {
        request("DELETE", fsUrl(path)).close()
    }

    public suspend fun copy(srcPath: String, dstPath: String): Unit = withContext(Dispatchers.IO) {
        request("POST", "${fsUrl(dstPath)}?copy") {
            setRequestProperty("X-Dat9-Copy-Source", normalizedPath(srcPath))
        }.close()
    }

    public suspend fun rename(oldPath: String, newPath: String): Unit = withContext(Dispatchers.IO) {
        request("POST", "${fsUrl(newPath)}?rename") {
            setRequestProperty("X-Dat9-Rename-Source", normalizedPath(oldPath))
        }.close()
    }

    public suspend fun mkdir(path: String): Unit = withContext(Dispatchers.IO) {
        request("POST", "${fsUrl(path)}?mkdir").close()
    }

    /** Semantic / full-text / keyword content search. */
    public suspend fun grep(
        query: String,
        pathPrefix: String,
        limit: Int = 0,
    ): List<Drive9SearchResult> = withContext(Dispatchers.IO) {
        val limitParam = if (limit > 0) "&limit=$limit" else ""
        val body = request("GET", "${fsUrl(pathPrefix)}?grep=${urlEncode(query)}$limitParam").use { it.text() }
        parseSearchResults(body)
    }

    /** Metadata search. `params` is forwarded as query params with `find=` added. */
    public suspend fun find(
        pathPrefix: String,
        params: Map<String, String> = emptyMap(),
    ): List<Drive9SearchResult> = withContext(Dispatchers.IO) {
        val query = (params + ("find" to ""))
            .entries
            .joinToString("&") { (k, v) -> "${urlEncode(k)}=${urlEncode(v)}" }
        val body = request("GET", "${fsUrl(pathPrefix)}?$query").use { it.text() }
        parseSearchResults(body)
    }

    /** Run a SQL query. Each row is returned as a compact JSON string. */
    public suspend fun sql(query: String): List<String> = withContext(Dispatchers.IO) {
        val payload = buildJsonObject { put("query", query) }.toString().toByteArray(StandardCharsets.UTF_8)
        val body = request("POST", "$baseUrlValue/v1/sql") {
            setRequestProperty("Content-Type", "application/json")
            doOutput = true
            outputStream.use { it.write(payload) }
        }.use { it.text() }
        json.parseToJsonElement(body).jsonArray.map { it.toString() }
    }

    public fun downloadFlow(
        remotePath: String,
        cancel: Drive9CancelToken? = null,
    ): Flow<ByteArray> = readFlow(remotePath, null, cancel)

    public fun downloadRangeFlow(
        remotePath: String,
        offset: Long,
        length: Long,
        cancel: Drive9CancelToken? = null,
    ): Flow<ByteArray> = readFlow(remotePath, offset to length, cancel)

    public suspend fun uploadFlow(
        remotePath: String,
        totalSize: Long,
        chunks: Flow<ByteArray>,
        expectedRevision: Long? = null,
        progress: Drive9ProgressListener? = null,
        token: Drive9CancelToken? = null,
    ): Unit {
        checkCancel(token)
        val writer = newStreamUpload(remotePath, totalSize, expectedRevision)
        var transferred = 0UL
        var partNum = 1
        val buffer = ByteArrayOutputStream()
        val partSize = writer.partSize().toInt()
        try {
            chunks.collect { chunk ->
                checkCancel(token)
                if (chunk.isEmpty()) return@collect
                buffer.write(chunk)
                while (buffer.size() >= partSize) {
                    val data = buffer.toByteArray()
                    writer.writePart(partNum, data.copyOfRange(0, partSize))
                    buffer.reset()
                    if (data.size > partSize) buffer.write(data, partSize, data.size - partSize)
                    transferred += partSize.toULong()
                    progress?.onProgress(transferred, totalSize.toULong())
                    partNum++
                }
            }
            val finalData = buffer.toByteArray()
            writer.complete(partNum, finalData)
            transferred += finalData.size.toULong()
            progress?.onProgress(transferred, totalSize.toULong())
        } catch (e: Throwable) {
            runCatching { writer.abort() }
            throw e
        } finally {
            writer.close()
        }
    }

    public suspend fun newStreamUpload(
        remotePath: String,
        totalSize: Long,
        expectedRevision: Long? = null,
    ): Drive9StreamUpload = withContext(Dispatchers.IO) {
        val plan = initiateUploadV2(remotePath, totalSize, expectedRevision ?: -1)
        Drive9StreamUpload(this@Drive9Client, plan)
    }

    public suspend fun uploadFile(
        localPath: String,
        remotePath: String,
        expectedRevision: Long? = null,
        progress: Drive9ProgressListener? = null,
        token: Drive9CancelToken? = null,
    ): Unit = withContext(Dispatchers.IO) {
        val file = File(localPath)
        val total = file.length().coerceAtLeast(0)
        try {
            if (total < smallFileThreshold) {
                checkCancel(token)
                val data = file.readBytes()
                writeWithRevision(remotePath, data, expectedRevision ?: -1)
                progress?.onProgress(data.size.toULong(), total.toULong())
                return@withContext
            }
            uploadSeekable(remotePath, file, total, expectedRevision ?: -1, progress, token)
        } catch (e: Drive9Exception) {
            throw e
        } catch (e: IOException) {
            throw Drive9Exception.Drive9("io", null, e.message ?: "I/O error", null)
        }
    }

    public suspend fun downloadFile(
        remotePath: String,
        localPath: String,
        progress: Drive9ProgressListener? = null,
        token: Drive9CancelToken? = null,
    ): Unit = withContext(Dispatchers.IO) {
        checkCancel(token)
        val target = File(localPath)
        val temp = File(target.parentFile ?: File("."), ".${target.name}.drive9-tmp-${System.nanoTime()}")
        var transferred = 0UL
        try {
            rawInput("GET", fsUrl(remotePath)).use { response ->
                val total = response.contentLength.toULong()
                temp.outputStream().use { output ->
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    while (true) {
                        checkCancel(token)
                        val n = response.input.read(buffer)
                        if (n < 0) break
                        output.write(buffer, 0, n)
                        transferred += n.toULong()
                        progress?.onProgress(transferred, total)
                    }
                }
            }
            if (target.exists()) target.delete()
            if (!temp.renameTo(target)) throw Drive9Exception.Drive9("io", null, "rename temp to $localPath failed", null)
        } catch (e: Throwable) {
            temp.delete()
            throw e
        }
    }

    public suspend fun patchFileParts(
        localPath: String,
        remotePath: String,
        dirtyParts: List<Int>,
        newSize: Long,
        partSize: Long,
        expectedRevision: Long? = null,
    ): Unit = withContext(Dispatchers.IO) {
        if (newSize < 0) throw Drive9Exception.Drive9("other", null, "patchFileParts: new_size must be non-negative", null)
        if (partSize <= 0) throw Drive9Exception.Drive9("other", null, "patchFileParts: part_size must be positive", null)
        if (dirtyParts.any { it <= 0 }) throw Drive9Exception.Drive9("other", null, "patchFileParts: dirty_parts must be 1-based positive part numbers", null)

        val payload = buildJsonObject {
            put("new_size", newSize)
            put("dirty_parts", buildJsonArray { dirtyParts.forEach { add(JsonPrimitive(it)) } })
            put("part_size", partSize)
            expectedRevision?.let { put("expected_revision", it) }
        }.toString().toByteArray(StandardCharsets.UTF_8)
        val plan = rawHttp(
            method = "PATCH",
            url = fsUrl(remotePath),
            headers = mapOf("Content-Type" to "application/json"),
            body = payload,
        ).let { response ->
            if (response.status !in 200..299) throw errorFrom(response.status, response.body)
            parsePatchPlan(response.body.toString(StandardCharsets.UTF_8))
        }

        RandomAccessFile(File(localPath), "r").use { raf ->
            for (part in plan.uploadParts) {
                val original = part.readUrl?.let { rawRequestBytes("GET", it, part.readHeaders) }
                val data = readLocalPart(raf, part.number, plan.partSize, part.size, original)
                uploadPatchPart(part, data)
            }
        }
        completeUpload(plan.uploadId)
    }

    public suspend fun resumeUpload(localPath: String, remotePath: String, totalSize: Long): Unit = withContext(Dispatchers.IO) {
        val meta = queryUpload(remotePath)
        val file = File(localPath)
        RandomAccessFile(file, "r").use { raf ->
            val checksums = computePartChecksums(raf, totalSize, DEFAULT_PART_SIZE)
            val plan = requestResume(meta.uploadId, checksums)
            for (part in plan.parts) {
                val partSize = if (plan.partSize > 0) plan.partSize else DEFAULT_PART_SIZE
                val data = readAt(raf, (part.number - 1L) * partSize, part.size.toInt())
                uploadOnePart(part, data)
            }
            completeUpload(plan.uploadId)
        }
    }

    public suspend fun vaultCreateSecret(name: String, fields: Map<String, JsonElement>): Drive9VaultSecret =
        createVaultSecret(name, fields)

    public suspend fun createVaultSecret(name: String, fields: Map<String, JsonElement>): Drive9VaultSecret =
        withContext(Dispatchers.IO) {
            val payload = buildJsonObject {
                put("name", name)
                put("fields", JsonObject(fields))
                put("created_by", "drive9-kotlin")
            }.toString().toByteArray(StandardCharsets.UTF_8)
            request("POST", vaultUrl("/secrets")) {
                setRequestProperty("Content-Type", "application/json")
                doOutput = true
                outputStream.use { it.write(payload) }
            }.use { parseVaultSecret(it.text()) }
        }

    public suspend fun updateVaultSecret(name: String, fields: Map<String, JsonElement>): Drive9VaultSecret =
        withContext(Dispatchers.IO) {
            val payload = buildJsonObject {
                put("fields", JsonObject(fields))
                put("updated_by", "drive9-kotlin")
            }.toString().toByteArray(StandardCharsets.UTF_8)
            request("PUT", vaultUrl("/secrets/${urlEncode(name)}")) {
                setRequestProperty("Content-Type", "application/json")
                doOutput = true
                outputStream.use { it.write(payload) }
            }.use { parseVaultSecret(it.text()) }
        }

    public suspend fun deleteVaultSecret(name: String): Unit = withContext(Dispatchers.IO) {
        request("DELETE", vaultUrl("/secrets/${urlEncode(name)}")).close()
    }

    public suspend fun listVaultSecrets(): List<Drive9VaultSecret> = withContext(Dispatchers.IO) {
        val body = request("GET", vaultUrl("/secrets")).use { it.text() }
        val entries = json.parseToJsonElement(body).jsonObject["secrets"] as? JsonArray ?: JsonArray(emptyList())
        entries.map { parseVaultSecret(it.toString()) }
    }

    public suspend fun issueVaultToken(
        agentId: String,
        taskId: String,
        scope: List<String>,
        ttlSeconds: Long,
    ): Drive9VaultTokenIssueResponse = withContext(Dispatchers.IO) {
        val payload = buildJsonObject {
            put("agent_id", agentId)
            put("task_id", taskId)
            put("scope", buildJsonArray { scope.forEach { add(JsonPrimitive(it)) } })
            put("ttl_seconds", ttlSeconds)
        }.toString().toByteArray(StandardCharsets.UTF_8)
        request("POST", vaultUrl("/tokens")) {
            setRequestProperty("Content-Type", "application/json")
            doOutput = true
            outputStream.use { it.write(payload) }
        }.use { parseVaultTokenIssueResponse(it.text()) }
    }

    public suspend fun revokeVaultToken(tokenId: String): Unit = withContext(Dispatchers.IO) {
        request("DELETE", vaultUrl("/tokens/${urlEncode(tokenId)}")).close()
    }

    public suspend fun queryVaultAudit(secretName: String? = null, limit: Int = 0): List<Drive9VaultAuditEvent> =
        withContext(Dispatchers.IO) {
            val params = mutableListOf<String>()
            secretName?.let { params += "secret=${urlEncode(it)}" }
            if (limit > 0) params += "limit=$limit"
            val suffix = if (params.isEmpty()) "" else "?${params.joinToString("&")}"
            val body = request("GET", vaultUrl("/audit$suffix")).use { it.text() }
            val entries = json.parseToJsonElement(body).jsonObject["events"] as? JsonArray ?: JsonArray(emptyList())
            entries.map { it.jsonObject.toVaultAuditEvent() }
        }

    public suspend fun vaultListReadableSecrets(): List<String> = listReadableVaultSecrets()

    public suspend fun listReadableVaultSecrets(): List<String> = withContext(Dispatchers.IO) {
        val body = request("GET", vaultUrl("/read")).use { it.text() }
        val entries = json.parseToJsonElement(body).jsonObject["secrets"] as? JsonArray ?: JsonArray(emptyList())
        entries.mapNotNull { it.jsonPrimitive.contentOrNull }
    }

    public suspend fun readVaultSecret(name: String): Map<String, JsonElement> = withContext(Dispatchers.IO) {
        val body = request("GET", vaultUrl("/read/${urlEncode(name)}")).use { it.text() }
        json.parseToJsonElement(body).jsonObject
    }

    public suspend fun vaultReadSecretField(name: String, field: String): String =
        readVaultSecretField(name, field)

    public suspend fun readVaultSecretField(name: String, field: String): String = withContext(Dispatchers.IO) {
        request("GET", vaultUrl("/read/${urlEncode(name)}/${urlEncode(field)}")).use { it.text() }
    }

    internal suspend fun uploadStreamPart(plan: Drive9UploadPlanV2, partNumber: Int, data: ByteArray): String =
        withContext(Dispatchers.IO) {
            val part = presignOnePart(plan.uploadId, partNumber)
            uploadOnePartV2(plan.uploadId, part, data)
        }

    internal suspend fun completeStreamUpload(uploadId: String, parts: List<Drive9CompletePart>) {
        completeUploadV2(uploadId, parts)
    }

    internal suspend fun abortStreamUpload(uploadId: String) {
        abortUploadV2(uploadId)
    }

    private fun readFlow(
        remotePath: String,
        range: Pair<Long, Long>?,
        cancel: Drive9CancelToken?,
    ): Flow<ByteArray> = flow {
        if (range != null && range.second <= 0) return@flow
        val conn = open("GET", fsUrl(remotePath))
        if (range != null) {
            conn.setRequestProperty("Range", "bytes=${range.first}-${range.first + range.second - 1}")
        }
        try {
            val status = conn.responseCode
            if (range != null && status == 416) return@flow
            if (status !in 200..299) throw errorFrom(conn, status)
            val input = conn.inputStream
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            input.use {
                while (true) {
                    checkCancel(cancel)
                    val n = withContext(Dispatchers.IO) { it.read(buffer) }
                    if (n < 0) break
                    emit(buffer.copyOf(n))
                }
            }
        } finally {
            conn.disconnect()
        }
    }

    private suspend fun uploadSeekable(
        remotePath: String,
        file: File,
        totalSize: Long,
        expectedRevision: Long,
        progress: Drive9ProgressListener?,
        token: Drive9CancelToken?,
    ) {
        RandomAccessFile(file, "r").use { raf ->
            try {
                uploadSeekableV2(remotePath, raf, totalSize, expectedRevision, progress, token)
            } catch (e: Drive9Exception.Drive9) {
                if (!e.detail.contains("v2 upload API not available")) throw e
                raf.seek(0)
                uploadSeekableV1(remotePath, raf, totalSize, expectedRevision, progress, token)
            }
        }
    }

    private fun uploadSeekableV1(
        remotePath: String,
        raf: RandomAccessFile,
        totalSize: Long,
        expectedRevision: Long,
        progress: Drive9ProgressListener?,
        token: Drive9CancelToken?,
    ) {
        val checksums = computePartChecksums(raf, totalSize, DEFAULT_PART_SIZE)
        val plan = initiateUpload(remotePath, totalSize, checksums, expectedRevision)
        var transferred = 0UL
        for (part in plan.parts) {
            checkCancel(token)
            val partSize = if (plan.partSize > 0) plan.partSize else DEFAULT_PART_SIZE
            val data = readAt(raf, (part.number - 1L) * partSize, part.size.toInt())
            uploadOnePart(part, data)
            transferred += data.size.toULong()
            progress?.onProgress(transferred, totalSize.toULong())
        }
        completeUpload(plan.uploadId)
    }

    private fun uploadSeekableV2(
        remotePath: String,
        raf: RandomAccessFile,
        totalSize: Long,
        expectedRevision: Long,
        progress: Drive9ProgressListener?,
        token: Drive9CancelToken?,
    ) {
        val plan = initiateUploadV2Blocking(remotePath, totalSize, expectedRevision)
        var transferred = 0UL
        val parts = mutableListOf<Drive9CompletePart>()
        try {
            for (partNumber in 1..plan.totalParts) {
                checkCancel(token)
                val offset = (partNumber - 1L) * plan.partSize
                val size = minOf(plan.partSize, totalSize - offset).toInt()
                val data = readAt(raf, offset, size)
                val presigned = presignOnePartBlocking(plan.uploadId, partNumber)
                val etag = uploadOnePartV2Blocking(plan.uploadId, presigned, data)
                parts += Drive9CompletePart(partNumber, etag)
                transferred += data.size.toULong()
                progress?.onProgress(transferred, totalSize.toULong())
            }
            completeUploadV2Blocking(plan.uploadId, parts)
        } catch (e: Throwable) {
            runCatching { abortUploadV2Blocking(plan.uploadId) }
            throw e
        }
    }

    private fun initiateUpload(
        path: String,
        size: Long,
        checksums: List<String>,
        expectedRevision: Long,
    ): Drive9UploadPlan {
        val payload = buildJsonObject {
            put("path", path)
            put("total_size", size)
            put("part_checksums", buildJsonArray { checksums.forEach { add(JsonPrimitive(it)) } })
            if (expectedRevision >= 0) put("expected_revision", expectedRevision)
        }.toString().toByteArray(StandardCharsets.UTF_8)
        val result = runCatching {
            val conn = open("POST", "$baseUrlValue/v1/uploads/initiate")
            try {
                conn.setRequestProperty("Content-Type", "application/json")
                conn.doOutput = true
                conn.outputStream.use { it.write(payload) }
                val status = conn.responseCode
                if (status == 202) return@runCatching NativeResponse(readResponseBytes(conn)).text()
                throw errorFrom(conn, status)
            } finally {
                conn.disconnect()
            }
        }
        return result.getOrElse { error ->
            val drive9 = error as? Drive9Exception.Drive9
            if (drive9 == null || (drive9.statusCode != 404 && drive9.statusCode != 405 && !(drive9.statusCode == 400 && drive9.detail.lowercase().contains("unknown upload action")))) {
                throw error
            }
            initiateUploadLegacy(path, size, checksums, expectedRevision)
        }.let { parseUploadPlan(it) }
    }

    private fun initiateUploadLegacy(
        path: String,
        size: Long,
        checksums: List<String>,
        expectedRevision: Long,
    ): String {
        return request("PUT", fsUrl(path)) {
            setRequestProperty("Content-Type", "application/octet-stream")
            setRequestProperty("X-Dat9-Content-Length", size.toString())
            if (checksums.isNotEmpty()) setRequestProperty("X-Dat9-Part-Checksums", checksums.joinToString(","))
            if (expectedRevision >= 0) setRequestProperty("X-Dat9-Expected-Revision", expectedRevision.toString())
        }.use { it.text() }
    }

    private suspend fun initiateUploadV2(path: String, size: Long, expectedRevision: Long): Drive9UploadPlanV2 =
        withContext(Dispatchers.IO) { initiateUploadV2Blocking(path, size, expectedRevision) }

    private fun initiateUploadV2Blocking(path: String, size: Long, expectedRevision: Long): Drive9UploadPlanV2 {
        val payload = buildJsonObject {
            put("path", path)
            put("total_size", size)
            if (expectedRevision >= 0) put("expected_revision", expectedRevision)
        }.toString().toByteArray(StandardCharsets.UTF_8)
        val conn = open("POST", "$baseUrlValue/v2/uploads/initiate")
        try {
            conn.setRequestProperty("Content-Type", "application/json")
            conn.doOutput = true
            conn.outputStream.use { it.write(payload) }
            val status = conn.responseCode
            if (status == 404) throw Drive9Exception.Drive9("other", 404, "v2 upload API not available", null)
            if (status !in 200..299) throw errorFrom(conn, status)
            return parseUploadPlanV2(readResponseText(conn))
        } finally {
            conn.disconnect()
        }
    }

    private suspend fun presignOnePart(uploadId: String, partNumber: Int): Drive9PresignedPart =
        withContext(Dispatchers.IO) { presignOnePartBlocking(uploadId, partNumber) }

    private fun presignOnePartBlocking(uploadId: String, partNumber: Int): Drive9PresignedPart {
        val payload = buildJsonObject { put("part_number", partNumber) }.toString().toByteArray(StandardCharsets.UTF_8)
        return request("POST", "$baseUrlValue/v2/uploads/$uploadId/presign") {
            setRequestProperty("Content-Type", "application/json")
            doOutput = true
            outputStream.use { it.write(payload) }
        }.use { parsePresignedPart(it.text()) }
    }

    private suspend fun uploadOnePartV2(uploadId: String, part: Drive9PresignedPart, data: ByteArray): String =
        withContext(Dispatchers.IO) { uploadOnePartV2Blocking(uploadId, part, data) }

    private fun uploadOnePartV2Blocking(uploadId: String, part: Drive9PresignedPart, data: ByteArray): String {
        val first = rawPut(part.url, part.headers, data, retryOnForbidden = false)
        if (first.status == 403) {
            val fresh = presignOnePartBlocking(uploadId, part.number)
            val retry = rawPut(fresh.url, fresh.headers, data, retryOnForbidden = false)
            if (retry.status !in 200..299) throw errorFrom(retry.status, retry.body)
            return retry.headers["etag"].orEmpty()
        }
        if (first.status !in 200..299) throw errorFrom(first.status, first.body)
        return first.headers["etag"].orEmpty()
    }

    private fun uploadOnePart(part: Drive9PartUrl, data: ByteArray): String {
        val headers = part.headers.toMutableMap()
        headers["x-amz-checksum-crc32c"] = part.checksumCrc32c ?: crc32cBase64(data)
        val result = rawPut(part.url, headers, data, retryOnForbidden = false)
        if (result.status !in 200..299) throw errorFrom(result.status, result.body)
        return result.headers["etag"].orEmpty()
    }

    private fun uploadPatchPart(part: Drive9PatchPartUrl, data: ByteArray) {
        val headers = part.headers.toMutableMap()
        headers["x-amz-checksum-sha256"] = sha256Base64(data)
        val result = rawPut(part.url, headers, data, retryOnForbidden = false)
        if (result.status !in 200..299) throw errorFrom(result.status, result.body)
    }

    private suspend fun completeUploadV2(uploadId: String, parts: List<Drive9CompletePart>) {
        withContext(Dispatchers.IO) { completeUploadV2Blocking(uploadId, parts) }
    }

    private fun completeUploadV2Blocking(uploadId: String, parts: List<Drive9CompletePart>) {
        val payload = buildJsonObject {
            put("parts", buildJsonArray {
                parts.forEach { p ->
                    add(buildJsonObject {
                        put("number", p.number)
                        put("etag", p.etag)
                    })
                }
            })
        }.toString().toByteArray(StandardCharsets.UTF_8)
        request("POST", "$baseUrlValue/v2/uploads/$uploadId/complete") {
            setRequestProperty("Content-Type", "application/json")
            doOutput = true
            outputStream.use { it.write(payload) }
        }.close()
    }

    private suspend fun abortUploadV2(uploadId: String) {
        withContext(Dispatchers.IO) { abortUploadV2Blocking(uploadId) }
    }

    private fun abortUploadV2Blocking(uploadId: String) {
        request("POST", "$baseUrlValue/v2/uploads/$uploadId/abort").close()
    }

    private fun completeUpload(uploadId: String) {
        request("POST", "$baseUrlValue/v1/uploads/$uploadId/complete").close()
    }

    private fun queryUpload(path: String): Drive9UploadMeta {
        val body = request("GET", "$baseUrlValue/v1/uploads?path=${urlEncode(path)}&status=UPLOADING").use { it.text() }
        val uploads = json.parseToJsonElement(body).jsonObject["uploads"] as? JsonArray ?: JsonArray(emptyList())
        val first = uploads.firstOrNull()?.jsonObject ?: throw Drive9Exception.Drive9("other", null, "no active upload for $path", null)
        return Drive9UploadMeta(
            uploadId = first["upload_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            partsTotal = first["parts_total"]?.jsonPrimitive?.longOrNull?.toInt() ?: 0,
            status = first["status"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            expiresAt = first["expires_at"]?.jsonPrimitive?.contentOrNull.orEmpty(),
        )
    }

    private fun requestResume(uploadId: String, checksums: List<String>): Drive9UploadPlan {
        val payload = buildJsonObject {
            put("part_checksums", buildJsonArray { checksums.forEach { add(JsonPrimitive(it)) } })
        }.toString().toByteArray(StandardCharsets.UTF_8)
        val result = runCatching {
            request("POST", "$baseUrlValue/v1/uploads/$uploadId/resume") {
                setRequestProperty("Content-Type", "application/json")
                doOutput = true
                outputStream.use { it.write(payload) }
            }.use { parseUploadPlan(it.text()) }
        }
        return result.getOrElse { error ->
            val drive9 = error as? Drive9Exception.Drive9
            if (drive9?.statusCode == 400 && drive9.detail.lowercase().contains("missing x-dat9-part-checksums header")) {
                request("POST", "$baseUrlValue/v1/uploads/$uploadId/resume") {
                    if (checksums.isNotEmpty()) setRequestProperty("X-Dat9-Part-Checksums", checksums.joinToString(","))
                }.use { parseUploadPlan(it.text()) }
            } else {
                throw error
            }
        }
    }

    private fun open(method: String, url: String): HttpURLConnection {
        val conn = URI(url).toURL().openConnection() as HttpURLConnection
        conn.requestMethod = method
        if (apiKeyValue.isNotEmpty()) {
            conn.setRequestProperty("Authorization", "Bearer $apiKeyValue")
        }
        conn.setRequestProperty("User-Agent", "drive9-kotlin-native/0.2")
        conn.connectTimeout = 30_000
        conn.readTimeout = 120_000
        return conn
    }

    private inline fun request(method: String, url: String, configure: HttpURLConnection.() -> Unit = {}): NativeResponse {
        val conn = open(method, url)
        try {
            conn.configure()
            val status = conn.responseCode
            if (status !in 200..299) throw errorFrom(conn, status)
            return NativeResponse(if (method == "HEAD" || status == HttpURLConnection.HTTP_NO_CONTENT) ByteArray(0) else readResponseBytes(conn))
        } finally {
            conn.disconnect()
        }
    }

    private fun rawInput(method: String, url: String): NativeInputResponse {
        val conn = open(method, url)
        val status = conn.responseCode
        if (status !in 200..299) {
            val err = errorFrom(conn, status)
            conn.disconnect()
            throw err
        }
        return NativeInputResponse(
            input = DisconnectingInputStream(conn.inputStream, conn),
            contentLength = conn.headerFieldLong("Content-Length") ?: 0L,
        )
    }

    private fun rawRequestBytes(method: String, url: String, headers: Map<String, String>): ByteArray {
        val conn = URI(url).toURL().openConnection() as HttpURLConnection
        conn.requestMethod = method
        headers.forEach { (k, v) -> if (!k.equals("host", ignoreCase = true)) conn.setRequestProperty(k, v) }
        try {
            val status = conn.responseCode
            val body = readResponseBytes(conn)
            if (status !in 200..299) throw errorFrom(status, body)
            return body
        } finally {
            conn.disconnect()
        }
    }

    private fun rawPut(url: String, headers: Map<String, String>, data: ByteArray, retryOnForbidden: Boolean): RawResponse {
        val conn = URI(url).toURL().openConnection() as HttpURLConnection
        conn.requestMethod = "PUT"
        headers.forEach { (k, v) -> if (!k.equals("host", ignoreCase = true)) conn.setRequestProperty(k, v) }
        conn.doOutput = true
        conn.setFixedLengthStreamingMode(data.size)
        try {
            conn.outputStream.use { it.write(data) }
            val status = conn.responseCode
            val body = if (status in 200..299) readResponseBytes(conn) else conn.errorStream?.use { it.readBytes() } ?: ByteArray(0)
            val responseHeaders = conn.headerFields
                .filterKeys { it != null }
                .mapKeys { it.key.lowercase() }
                .mapValues { it.value.firstOrNull().orEmpty() }
            return RawResponse(status, body, responseHeaders)
        } finally {
            conn.disconnect()
        }
    }

    private fun rawHttp(method: String, url: String, headers: Map<String, String>, body: ByteArray): RawResponse {
        // HttpURLConnection rejects PATCH on JVM/Android. Keep this tiny
        // HTTP/1.1 fallback scoped to the patch endpoint instead of using it
        // as a general-purpose transport.
        val uri = URI(url)
        val ssl = uri.scheme.equals("https", ignoreCase = true)
        val port = if (uri.port > 0) uri.port else if (ssl) 443 else 80
        val host = uri.host ?: throw Drive9Exception.Drive9("request", null, "missing host in URL: $url", null)
        val socket = if (ssl) {
            SSLSocketFactory.getDefault().createSocket(host, port)
        } else {
            Socket(host, port)
        }
        socket.use { s ->
            s.soTimeout = 120_000
            val target = buildString {
                append(uri.rawPath.ifEmpty { "/" })
                if (!uri.rawQuery.isNullOrEmpty()) append("?").append(uri.rawQuery)
            }
            val allHeaders = linkedMapOf<String, String>()
            allHeaders["Host"] = host
            allHeaders["User-Agent"] = "drive9-kotlin-native/0.2"
            apiKeyValue.takeIf { it.isNotEmpty() }?.let { allHeaders["Authorization"] = "Bearer $it" }
            headers.forEach { (k, v) -> if (!k.equals("host", ignoreCase = true)) allHeaders[k] = v }
            allHeaders["Content-Length"] = body.size.toString()
            allHeaders["Connection"] = "close"

            val header = buildString {
                append(method).append(" ").append(target).append(" HTTP/1.1\r\n")
                allHeaders.forEach { (k, v) -> append(k).append(": ").append(v).append("\r\n") }
                append("\r\n")
            }.toByteArray(StandardCharsets.UTF_8)
            val output = s.getOutputStream()
            output.write(header)
            output.write(body)
            output.flush()
            val input = s.getInputStream()
            val responseBytes = input.readBytes()
            val split = responseBytes.indexOfHeaderEnd()
            val headerText = responseBytes.copyOfRange(0, split).toString(StandardCharsets.ISO_8859_1)
            val bodyStart = split + 4
            val responseBody = if (bodyStart <= responseBytes.size) responseBytes.copyOfRange(bodyStart, responseBytes.size) else ByteArray(0)
            val lines = headerText.split("\r\n")
            val status = lines.firstOrNull()?.split(" ")?.getOrNull(1)?.toIntOrNull() ?: 0
            val responseHeaders = lines.drop(1).mapNotNull { line ->
                val idx = line.indexOf(':')
                if (idx <= 0) null else line.substring(0, idx).lowercase() to line.substring(idx + 1).trim()
            }.toMap()
            return RawResponse(status, responseBody, responseHeaders)
        }
    }

    private fun errorFrom(conn: HttpURLConnection, status: Int): Drive9Exception.Drive9 {
        val bytes = conn.errorStream?.use { it.readBytes() } ?: ByteArray(0)
        return errorFrom(status, bytes)
    }

    private fun errorFrom(status: Int, bytes: ByteArray): Drive9Exception.Drive9 {
        val text = bytes.toString(StandardCharsets.UTF_8)
        val parsed = runCatching { json.parseToJsonElement(text).jsonObject }.getOrNull()
        val message = parsed?.get("error")?.jsonPrimitive?.contentOrNull
            ?: parsed?.get("message")?.jsonPrimitive?.contentOrNull
            ?: text.ifBlank { "HTTP $status" }
        val serverRevision = parsed?.get("server_revision")?.jsonPrimitive?.longOrNull
        val code = if (status == 409) "conflict" else "http_status"
        return Drive9Exception.Drive9(code, status, message, serverRevision)
    }

    private fun fsUrl(path: String): String = "$baseUrlValue/v1/fs${pathEncode(normalizedPath(path))}"

    private fun vaultUrl(path: String): String = "$baseUrlValue/v1/vault${if (path.startsWith('/')) path else "/$path"}"

    private fun normalizedPath(path: String): String = if (path.startsWith('/')) path else "/$path"

    private fun parseSearchResults(body: String): List<Drive9SearchResult> =
        json.parseToJsonElement(body).jsonArray.map { it.jsonObject.toSearchResult() }

    private fun parseUploadPlan(body: String): Drive9UploadPlan {
        val obj = json.parseToJsonElement(body).jsonObject
        val parts = obj["parts"]?.jsonArray?.map { it.jsonObject.toPartUrl() } ?: emptyList()
        return Drive9UploadPlan(
            uploadId = obj["upload_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            partSize = obj["part_size"]?.jsonPrimitive?.longOrNull ?: 0L,
            parts = parts,
        )
    }

    private fun parseUploadPlanV2(body: String): Drive9UploadPlanV2 {
        val obj = json.parseToJsonElement(body).jsonObject
        return Drive9UploadPlanV2(
            uploadId = obj["upload_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            key = obj["key"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            partSize = obj["part_size"]?.jsonPrimitive?.longOrNull ?: DEFAULT_PART_SIZE,
            totalParts = obj["total_parts"]?.jsonPrimitive?.longOrNull?.toInt() ?: 0,
        )
    }

    private fun parsePresignedPart(body: String): Drive9PresignedPart {
        val obj = json.parseToJsonElement(body).jsonObject
        return Drive9PresignedPart(
            number = obj["number"]?.jsonPrimitive?.longOrNull?.toInt() ?: 0,
            url = obj["url"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            size = obj["size"]?.jsonPrimitive?.longOrNull ?: 0L,
            headers = obj["headers"]?.jsonObject?.toStringMap() ?: emptyMap(),
        )
    }

    private fun parsePatchPlan(body: String): Drive9PatchPlan {
        val obj = json.parseToJsonElement(body).jsonObject
        return Drive9PatchPlan(
            uploadId = obj["upload_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            partSize = obj["part_size"]?.jsonPrimitive?.longOrNull ?: DEFAULT_PART_SIZE,
            uploadParts = obj["upload_parts"]?.jsonArray?.map { it.jsonObject.toPatchPartUrl() } ?: emptyList(),
            copiedParts = obj["copied_parts"]?.jsonArray?.mapNotNull { it.jsonPrimitive.longOrNull?.toInt() } ?: emptyList(),
        )
    }

    private fun parseVaultSecret(body: String): Drive9VaultSecret {
        val obj = json.parseToJsonElement(body).jsonObject
        return Drive9VaultSecret(
            name = obj["name"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            secretType = obj["secret_type"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            revision = obj["revision"]?.jsonPrimitive?.longOrNull ?: 0L,
            createdBy = obj["created_by"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            createdAt = obj["created_at"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            updatedAt = obj["updated_at"]?.jsonPrimitive?.contentOrNull.orEmpty(),
        )
    }

    private fun parseVaultTokenIssueResponse(body: String): Drive9VaultTokenIssueResponse {
        val obj = json.parseToJsonElement(body).jsonObject
        return Drive9VaultTokenIssueResponse(
            token = obj["token"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            tokenId = obj["token_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            expiresAt = obj["expires_at"]?.jsonPrimitive?.contentOrNull.orEmpty(),
        )
    }
}

public class Drive9StreamUpload internal constructor(
    private val client: Drive9Client,
    private val plan: Drive9UploadPlanV2,
) : AutoCloseable {
    private val uploaded = linkedMapOf<Int, Drive9CompletePart>()
    private var completed = false
    private var aborted = false

    public suspend fun writePart(partNum: Int, data: ByteArray) {
        ensureOpen()
        if (partNum < 1) throw Drive9Exception.Drive9("other", null, "part number must be >= 1", null)
        if (partNum > plan.totalParts) {
            throw Drive9Exception.Drive9("other", null, "part number $partNum exceeds total_parts ${plan.totalParts}", null)
        }
        if (uploaded.containsKey(partNum)) {
            throw Drive9Exception.Drive9("other", null, "part $partNum already uploaded", null)
        }
        val etag = client.uploadStreamPart(plan, partNum, data)
        uploaded[partNum] = Drive9CompletePart(partNum, etag)
    }

    public suspend fun complete(finalPartNum: Int, finalData: ByteArray) {
        ensureOpen()
        if (finalData.isNotEmpty()) writePart(finalPartNum, finalData)
        if (uploaded.isEmpty()) throw Drive9Exception.Drive9("other", null, "no parts uploaded in stream upload", null)
        val maxPart = uploaded.keys.max()
        val parts = (1..maxPart).map { part ->
            uploaded[part] ?: throw Drive9Exception.Drive9("other", null, "missing part $part in stream upload", null)
        }
        completed = true
        client.completeStreamUpload(plan.uploadId, parts)
    }

    public suspend fun abort() {
        if (aborted) return
        aborted = true
        if (!completed) client.abortStreamUpload(plan.uploadId)
    }

    public suspend fun partSize(): Long = plan.partSize

    public suspend fun totalParts(): Int = plan.totalParts

    override fun close() {}

    private fun ensureOpen() {
        if (completed) throw Drive9Exception.Drive9("other", null, "stream writer already completed", null)
        if (aborted) throw Drive9Exception.Drive9("other", null, "stream writer already aborted", null)
    }
}

public class Drive9StreamDownload internal constructor(private val chunks: Iterator<ByteArray>) : AutoCloseable {
    public fun readChunk(): ByteArray? = if (chunks.hasNext()) chunks.next() else null
    public fun closeStream() {}
    override fun close() {}
}

public class Drive9CancelToken {
    private val cancelled = AtomicBoolean(false)
    public fun cancel() { cancelled.set(true) }
    public fun isCancelled(): Boolean = cancelled.get()
    public fun close() {}
}

public interface Drive9ProgressListener {
    public fun onProgress(transferred: ULong, total: ULong)
}

public data class Drive9FileInfo(
    val name: String,
    val size: Long,
    val isDir: Boolean,
    val mtimeUnix: Long?,
)

public data class Drive9StatResult(
    val size: Long,
    val isDir: Boolean,
    val revision: Long,
    val mtimeUnix: Long?,
)

public data class Drive9SearchResult(
    val path: String,
    val name: String,
    val sizeBytes: Long,
    val score: Double?,
)

public data class Drive9PartUrl(
    val number: Int,
    val url: String,
    val size: Long,
    val checksumSha256: String?,
    val checksumCrc32c: String?,
    val headers: Map<String, String>,
    val expiresAt: String?,
)

public data class Drive9UploadPlan(
    val uploadId: String,
    val partSize: Long,
    val parts: List<Drive9PartUrl>,
)

public data class Drive9PatchPartUrl(
    val number: Int,
    val url: String,
    val size: Long,
    val headers: Map<String, String>,
    val expiresAt: String?,
    val readUrl: String?,
    val readHeaders: Map<String, String>,
)

public data class Drive9PatchPlan(
    val uploadId: String,
    val partSize: Long,
    val uploadParts: List<Drive9PatchPartUrl>,
    val copiedParts: List<Int>,
)

public data class Drive9UploadMeta(
    val uploadId: String,
    val partsTotal: Int,
    val status: String,
    val expiresAt: String,
)

public data class Drive9VaultSecret(
    val name: String,
    val secretType: String,
    val revision: Long,
    val createdBy: String,
    val createdAt: String,
    val updatedAt: String,
)

public data class Drive9VaultTokenIssueResponse(
    val token: String,
    val tokenId: String,
    val expiresAt: String,
)

public data class Drive9VaultAuditEvent(
    val eventId: String,
    val eventType: String,
    val timestamp: String,
    val tokenId: String?,
    val agentId: String?,
    val taskId: String?,
    val secretName: String?,
    val fieldName: String?,
    val adapter: String?,
    val detail: JsonElement?,
)

internal data class Drive9UploadPlanV2(
    val uploadId: String,
    val key: String,
    val partSize: Long,
    val totalParts: Int,
)

internal data class Drive9PresignedPart(
    val number: Int,
    val url: String,
    val size: Long,
    val headers: Map<String, String>,
)

internal data class Drive9CompletePart(val number: Int, val etag: String)

public sealed class Drive9Exception(message: String) : Exception(message) {
    public class Drive9(
        public val code: String,
        public val statusCode: Int?,
        public val detail: String,
        public val serverRevision: Long?,
    ) : Drive9Exception(detail)
}

private class NativeResponse(val body: ByteArray) : AutoCloseable {
    fun text(): String = body.toString(StandardCharsets.UTF_8)
    override fun close() {}
}

private class NativeInputResponse(
    val input: java.io.InputStream,
    val contentLength: Long,
) : AutoCloseable {
    override fun close() {
        input.close()
    }
}

private data class RawResponse(val status: Int, val body: ByteArray, val headers: Map<String, String>)

private class DisconnectingInputStream(
    private val inner: java.io.InputStream,
    private val conn: HttpURLConnection,
) : java.io.InputStream() {
    override fun read(): Int = inner.read()
    override fun read(b: ByteArray, off: Int, len: Int): Int = inner.read(b, off, len)
    override fun close() {
        inner.close()
        conn.disconnect()
    }
}

private fun HttpURLConnection.headerFieldLong(name: String): Long? = getHeaderField(name)?.toLongOrNull()

private fun HttpURLConnection.headerFieldUnixTime(name: String): Long? {
    val value = getHeaderField(name) ?: return null
    return value.toLongOrNull() ?: try {
        Instant.parse(value).epochSecond
    } catch (_: DateTimeParseException) {
        null
    }
}

private fun JsonObject.toFileInfo(): Drive9FileInfo = Drive9FileInfo(
    name = this["name"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    size = this["size"]?.jsonPrimitive?.longOrNull ?: 0L,
    isDir = this["isDir"]?.jsonPrimitive?.booleanOrNull ?: false,
    mtimeUnix = this["mtime"]?.jsonPrimitive?.longOrNull,
)

private fun JsonObject.toSearchResult(): Drive9SearchResult = Drive9SearchResult(
    path = this["path"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    name = this["name"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    sizeBytes = this["size_bytes"]?.jsonPrimitive?.longOrNull ?: 0L,
    score = this["score"]?.takeUnless { it is JsonNull }?.jsonPrimitive?.doubleOrNull,
)

private fun JsonObject.toPartUrl(): Drive9PartUrl = Drive9PartUrl(
    number = this["number"]?.jsonPrimitive?.longOrNull?.toInt() ?: 0,
    url = this["url"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    size = this["size"]?.jsonPrimitive?.longOrNull ?: 0L,
    checksumSha256 = this["checksum_sha256"]?.jsonPrimitive?.contentOrNull,
    checksumCrc32c = this["checksum_crc32c"]?.jsonPrimitive?.contentOrNull,
    headers = this["headers"]?.jsonObject?.toStringMap() ?: emptyMap(),
    expiresAt = this["expires_at"]?.jsonPrimitive?.contentOrNull,
)

private fun JsonObject.toPatchPartUrl(): Drive9PatchPartUrl = Drive9PatchPartUrl(
    number = this["number"]?.jsonPrimitive?.longOrNull?.toInt() ?: 0,
    url = this["url"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    size = this["size"]?.jsonPrimitive?.longOrNull ?: 0L,
    headers = this["headers"]?.jsonObject?.toStringMap() ?: emptyMap(),
    expiresAt = this["expires_at"]?.jsonPrimitive?.contentOrNull,
    readUrl = this["read_url"]?.jsonPrimitive?.contentOrNull,
    readHeaders = this["read_headers"]?.jsonObject?.toStringMap() ?: emptyMap(),
)

private fun JsonObject.toVaultAuditEvent(): Drive9VaultAuditEvent = Drive9VaultAuditEvent(
    eventId = this["event_id"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    eventType = this["event_type"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    timestamp = this["timestamp"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    tokenId = this["token_id"]?.jsonPrimitive?.contentOrNull,
    agentId = this["agent_id"]?.jsonPrimitive?.contentOrNull,
    taskId = this["task_id"]?.jsonPrimitive?.contentOrNull,
    secretName = this["secret_name"]?.jsonPrimitive?.contentOrNull,
    fieldName = this["field_name"]?.jsonPrimitive?.contentOrNull,
    adapter = this["adapter"]?.jsonPrimitive?.contentOrNull,
    detail = this["detail"],
)

private fun JsonObject.toStringMap(): Map<String, String> =
    entries.associate { (k, v) -> k to (v.jsonPrimitive.contentOrNull ?: v.toString()) }

private fun urlEncode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8.toString()).replace("+", "%20")

private fun pathEncode(value: String): String =
    value.split('/').joinToString("/") { urlEncode(it) }

private fun checkCancel(token: Drive9CancelToken?) {
    if (token?.isCancelled() == true) {
        throw Drive9Exception.Drive9("cancelled", null, "operation cancelled", null)
    }
}

private fun readResponseBytes(conn: HttpURLConnection): ByteArray =
    (conn.inputStream ?: conn.errorStream)?.use { it.readBytes() } ?: ByteArray(0)

private fun readResponseText(conn: HttpURLConnection): String =
    readResponseBytes(conn).toString(StandardCharsets.UTF_8)

private fun readAt(raf: RandomAccessFile, offset: Long, size: Int): ByteArray {
    if (size <= 0) return ByteArray(0)
    val data = ByteArray(size)
    raf.seek(offset)
    var read = 0
    while (read < size) {
        val n = raf.read(data, read, size - read)
        if (n < 0) break
        read += n
    }
    return if (read == size) data else data.copyOf(read)
}

private fun ByteArray.indexOfHeaderEnd(): Int {
    for (i in 0..(size - 4)) {
        if (this[i] == '\r'.code.toByte() &&
            this[i + 1] == '\n'.code.toByte() &&
            this[i + 2] == '\r'.code.toByte() &&
            this[i + 3] == '\n'.code.toByte()
        ) {
            return i
        }
    }
    return size
}

private fun readLocalPart(
    raf: RandomAccessFile,
    partNumber: Int,
    partSize: Long,
    requestedSize: Long,
    original: ByteArray?,
): ByteArray {
    val data = readAt(raf, (partNumber - 1L) * partSize, requestedSize.toInt())
    return if (data.isEmpty() && original != null) original else data
}

private fun computePartChecksums(raf: RandomAccessFile, totalSize: Long, partSize: Long): List<String> {
    val out = mutableListOf<String>()
    var offset = 0L
    while (offset < totalSize) {
        val size = minOf(partSize, totalSize - offset).toInt()
        out += crc32cBase64(readAt(raf, offset, size))
        offset += size
    }
    return out
}

private fun crc32cBase64(data: ByteArray): String {
    val crc = CRC32C()
    crc.update(data)
    val value = crc.value.toInt()
    val bytes = byteArrayOf(
        ((value ushr 24) and 0xff).toByte(),
        ((value ushr 16) and 0xff).toByte(),
        ((value ushr 8) and 0xff).toByte(),
        (value and 0xff).toByte(),
    )
    return Base64.getEncoder().encodeToString(bytes)
}

private fun sha256Base64(data: ByteArray): String =
    Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(data))
