package com.drive9.mobile

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.readBytes
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * JVM-runnable smoke tests against an in-process [HttpServer]. The goal is to
 * exercise the native HTTP surface without needing an Android device.
 */
class Drive9Test {
    private lateinit var server: HttpServer
    private lateinit var baseUrl: String
    private val routes = mutableMapOf<String, HttpHandler>()

    @BeforeTest
    fun startServer() {
        routes.clear()
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            val key = "${ex.requestMethod} ${ex.requestURI.rawPath}${ex.requestURI.rawQuery?.let { "?$it" } ?: ""}"
            val handler = routes[key]
            if (handler == null) {
                ex.sendResponseHeaders(404, -1)
                ex.close()
                return@createContext
            }
            handler.handle(ex)
        }
        server.start()
        baseUrl = "http://127.0.0.1:${server.address.port}"
    }

    @AfterTest
    fun stopServer() {
        server.stop(0)
    }

    private fun route(method: String, path: String, handler: (HttpExchange) -> Unit) {
        routes["$method $path"] = HttpHandler { ex -> handler(ex) }
    }

    private class RecordingProgress : Drive9ProgressListener {
        val events = mutableListOf<Pair<ULong, ULong>>()
        override fun onProgress(transferred: ULong, total: ULong) {
            events += transferred to total
        }
    }

    @Test
    fun writeThenReadRoundtrip() = runBlocking {
        route("PUT", "/v1/fs/hello.txt") { ex ->
            assertEquals("Bearer test-key", ex.requestHeaders.getFirst("Authorization"))
            assertEquals("7", ex.requestHeaders.getFirst("X-Dat9-Expected-Revision"))
            assertContentEquals("hello kotlin".toByteArray(), ex.requestBody.readBytes())
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("GET", "/v1/fs/hello.txt") { ex ->
            val body = "hello kotlin".toByteArray(StandardCharsets.UTF_8)
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "test-key")
        client.write("hello.txt", "hello kotlin".toByteArray(), expectedRevision = 7)
        val data = client.read("/hello.txt")
        assertContentEquals("hello kotlin".toByteArray(), data)
    }

    @Test
    fun emptyApiKeyDoesNotSendAuthorizationHeader() = runBlocking {
        route("GET", "/v1/fs/public.txt") { ex ->
            assertEquals(null, ex.requestHeaders.getFirst("Authorization"))
            val body = "public".toByteArray(StandardCharsets.UTF_8)
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "")
        assertContentEquals("public".toByteArray(), client.read("/public.txt"))
    }

    @Test
    fun listReturnsEntries() = runBlocking {
        route("GET", "/v1/fs/data/?list=1") { ex ->
            val body = """{"entries":[{"name":"a.txt","size":3,"isDir":false},{"name":"sub","size":0,"isDir":true}]}"""
                .toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val entries = client.list("/data/")
        assertEquals(2, entries.size)
        assertEquals("a.txt", entries[0].name)
        assertEquals(3, entries[0].size)
        assertFalse(entries[0].isDir)
        assertTrue(entries[1].isDir)
    }

    @Test
    fun statReportsRevisionAndSize() = runBlocking {
        route("HEAD", "/v1/fs/f.bin") { ex ->
            ex.responseHeaders.add("Content-Length", "42")
            ex.responseHeaders.add("X-Dat9-Revision", "9")
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val s = client.stat("/f.bin")
        assertEquals(42, s.size)
        assertEquals(9, s.revision)
        assertFalse(s.isDir)
    }

    @Test
    fun deleteSucceeds() = runBlocking {
        route("DELETE", "/v1/fs/gone.txt") { ex ->
            ex.sendResponseHeaders(204, -1)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        client.delete("/gone.txt")
    }

    @Test
    fun conflictPreservesServerRevision() = runBlocking {
        route("PUT", "/v1/fs/r.txt") { ex ->
            ex.requestBody.readBytes()
            val body = """{"error":"revision mismatch","server_revision":12}"""
                .toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(409, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val err = assertFailsWith<Drive9Exception.Drive9> {
            client.write("/r.txt", "x".toByteArray(), expectedRevision = 7L)
        }
        assertEquals("conflict", err.code)
        assertEquals(409, err.statusCode)
        assertEquals(12L, err.serverRevision)
    }

    @Test
    fun copyRenameMkdirSucceed() = runBlocking {
        route("POST", "/v1/fs/dst.txt?copy") { ex ->
            assertEquals("/src.txt", ex.requestHeaders.getFirst("X-Dat9-Copy-Source"))
            ex.sendResponseHeaders(200, -1); ex.close()
        }
        route("POST", "/v1/fs/new.txt?rename") { ex ->
            assertEquals("/old.txt", ex.requestHeaders.getFirst("X-Dat9-Rename-Source"))
            ex.sendResponseHeaders(200, -1); ex.close()
        }
        route("POST", "/v1/fs/dir/?mkdir") { ex ->
            ex.sendResponseHeaders(200, -1); ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        client.copy("/src.txt", "/dst.txt")
        client.rename("/old.txt", "/new.txt")
        client.mkdir("/dir/")
    }

    @Test
    fun grepReturnsSearchResults() = runBlocking {
        route("GET", "/v1/fs/?grep=hello&limit=3") { ex ->
            val body = """[{"path":"/a.txt","name":"a.txt","size_bytes":7,"score":0.5}]"""
                .toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body); ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val hits = client.grep("hello", "/", 3)
        assertEquals(1, hits.size)
        assertEquals("/a.txt", hits[0].path)
        assertEquals(7L, hits[0].sizeBytes)
        assertEquals(0.5, hits[0].score)
    }

    @Test
    fun findForwardsParams() = runBlocking {
        // HashMap iteration order is not deterministic; match each piece on
        // the server side instead of assuming a fixed query string.
        server.createContext("/v1/fs/data/") { ex ->
            val query = ex.requestURI.rawQuery.orEmpty()
            assertTrue("find=" in query, "missing find=: $query")
            assertTrue("type=file" in query, "missing type=file: $query")
            assertTrue("limit=10" in query, "missing limit=10: $query")
            val body = """[{"path":"/data/x.txt","name":"x.txt","size_bytes":1,"score":null}]"""
                .toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body); ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val hits = client.find("/data/", mapOf("type" to "file", "limit" to "10"))
        assertEquals(1, hits.size)
        assertEquals("/data/x.txt", hits[0].path)
    }

    @Test
    fun sqlReturnsJsonStrings() = runBlocking {
        route("POST", "/v1/sql") { ex ->
            ex.requestBody.readBytes()
            val body = """[{"path":"/a.txt","size":10},{"path":"/b","size":0}]"""
                .toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body); ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val rows = client.sql("SELECT path, size FROM files")
        assertEquals(2, rows.size)
        // The wrapper does not interpret schema; we just confirm each row is
        // valid JSON that we can parse back to inspect a field.
        assertTrue("\"path\":\"/a.txt\"" in rows[0])
        assertTrue("\"size\":10" in rows[0])
        assertTrue("\"path\":\"/b\"" in rows[1])
    }

    @Test
    fun downloadFileRoundtrip() = runBlocking {
        val body = ByteArray(200_000) { 'a'.code.toByte() }
        route("GET", "/v1/fs/big.bin") { ex ->
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body); ex.close()
        }

        val dest = Files.createTempFile("drive9-kotlin-download", ".bin")

        val client = Drive9Client(baseUrl, "k")
        try {
            client.downloadFile("/big.bin", dest.toString())
            assertContentEquals(body, dest.readBytes())
        } finally {
            dest.deleteIfExists()
        }
    }

    @Test
    fun downloadFileProgressUsesContentLength() = runBlocking {
        val body = "progress".toByteArray()
        route("GET", "/v1/fs/progress.bin") { ex ->
            ex.responseHeaders.add("Content-Length", body.size.toString())
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body); ex.close()
        }

        val dest = Files.createTempFile("drive9-kotlin-progress", ".bin")
        val progress = RecordingProgress()
        val client = Drive9Client(baseUrl, "k")
        try {
            client.downloadFile("/progress.bin", dest.toString(), progress = progress)
            assertEquals(body.size.toULong(), progress.events.last().first)
            assertEquals(body.size.toULong(), progress.events.last().second)
        } finally {
            dest.deleteIfExists()
        }
    }

    @Test
    fun downloadFileCancelBeforeLeavesNoFileWhenDestinationDidNotExist() = runBlocking {
        val parent = Files.createTempDirectory("drive9-kotlin-cancel-dir")
        val dest = parent.resolve("nonexistent.bin")
        val token = Drive9CancelToken()
        token.cancel()

        val client = Drive9Client(baseUrl, "k")
        try {
            val err = assertFailsWith<Drive9Exception.Drive9> {
                client.downloadFile("/cancel.bin", dest.toString(), null, token)
            }
            assertEquals("cancelled", err.code)
            assertFalse(dest.exists(), "destination must not be created on cancel")
            val leftover = Files.list(parent).use { it.toList() }
            assertEquals(emptyList(), leftover, "no leftover temp files expected")
        } finally {
            dest.deleteIfExists()
            Files.delete(parent)
            token.close()
        }
    }

    @Test
    fun uploadFileSmallRoundtrip() = runBlocking {
        val received = mutableListOf<ByteArray>()
        route("PUT", "/v1/fs/up.bin") { ex ->
            received.add(ex.requestBody.readBytes())
            ex.sendResponseHeaders(200, -1); ex.close()
        }

        val local = Files.createTempFile("drive9-kotlin-upload", ".bin")
        Files.write(local, ByteArray(100) { 'x'.code.toByte() })

        val client = Drive9Client(baseUrl, "k")
        try {
            client.uploadFile(local.toString(), "/up.bin")
            assertEquals(1, received.size)
            assertContentEquals(ByteArray(100) { 'x'.code.toByte() }, received[0])
        } finally {
            local.deleteIfExists()
        }
    }

    @Test
    fun uploadFileFallsBackToV1MultipartWhenV2Unavailable() = runBlocking {
        val received = mutableListOf<ByteArray>()
        route("POST", "/v2/uploads/initiate") { ex ->
            ex.requestBody.readBytes()
            ex.sendResponseHeaders(404, -1)
            ex.close()
        }
        route("POST", "/v1/uploads/initiate") { ex ->
            val body = ex.requestBody.readBytes().toString(StandardCharsets.UTF_8)
            assertTrue("\"part_checksums\"" in body)
            val response = """{"upload_id":"v1","part_size":4,"parts":[{"number":1,"url":"$baseUrl/v1-part","size":4,"headers":{}}]}"""
                .toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(202, response.size.toLong())
            ex.responseBody.write(response)
            ex.close()
        }
        route("PUT", "/v1-part") { ex ->
            assertTrue(ex.requestHeaders.getFirst("x-amz-checksum-crc32c") != null)
            received += ex.requestBody.readBytes()
            ex.responseHeaders.add("etag", "e1")
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("POST", "/v1/uploads/v1/complete") { ex ->
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val local = Files.createTempFile("drive9-kotlin-v1-fallback", ".bin")
        Files.write(local, "abcd".toByteArray())
        val client = Drive9Client(baseUrl, "k").withSmallFileThreshold(1)
        try {
            client.uploadFile(local.toString(), "/fallback.bin")
            assertContentEquals("abcd".toByteArray(), received.single())
        } finally {
            local.deleteIfExists()
        }
    }

    @Test
    fun uploadFileCancelBeforeReturnsCancelled() = runBlocking {
        var putHits = 0
        route("PUT", "/v1/fs/up-cancel.bin") { ex ->
            putHits++
            ex.sendResponseHeaders(200, -1); ex.close()
        }

        val local = Files.createTempFile("drive9-kotlin-upload-cancel", ".bin")
        Files.write(local, ByteArray(100) { 'y'.code.toByte() })
        val token = Drive9CancelToken()
        token.cancel()

        val client = Drive9Client(baseUrl, "k")
        try {
            val err = assertFailsWith<Drive9Exception.Drive9> {
                client.uploadFile(local.toString(), "/up-cancel.bin", null, null, token)
            }
            assertEquals("cancelled", err.code)
            assertEquals(0, putHits, "PUT should not happen when cancel is pre-set")
        } finally {
            local.deleteIfExists()
            token.close()
        }
    }

    @Test
    fun downloadFilePreservesPreexistingDestinationOnFailure() = runBlocking {
        route("GET", "/v1/fs/cancel.bin") { ex ->
            val body = """{"error":"boom"}""".toByteArray()
            ex.sendResponseHeaders(500, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val dest = Files.createTempFile("drive9-kotlin-preserve", ".bin")
        val original = "do not overwrite me".toByteArray()
        Files.write(dest, original)

        val client = Drive9Client(baseUrl, "k")
        try {
            val err = assertFailsWith<Drive9Exception.Drive9> {
                client.downloadFile("/cancel.bin", dest.toString())
            }
            assertEquals("http_status", err.code)
            assertContentEquals(original, dest.readBytes())
        } finally {
            dest.deleteIfExists()
        }
    }

    @Test
    fun patchFilePartsValidatesInputs() = runBlocking {
        val local = Files.createTempFile("drive9-kotlin-patch-validate", ".bin")
        Files.write(local, byteArrayOf('x'.code.toByte(), 'x'.code.toByte()))
        val client = Drive9Client(baseUrl, "k")
        try {
            val e1 = assertFailsWith<Drive9Exception.Drive9> {
                client.patchFileParts(local.toString(), "/r", listOf(1), -1L, 100L, null)
            }
            assertEquals("other", e1.code)
            assertTrue("new_size" in e1.detail, "want new_size error: ${e1.detail}")

            val e2 = assertFailsWith<Drive9Exception.Drive9> {
                client.patchFileParts(local.toString(), "/r", listOf(1), 100L, 0L, null)
            }
            assertEquals("other", e2.code)
            assertTrue("part_size" in e2.detail, "want part_size error: ${e2.detail}")

            val e3 = assertFailsWith<Drive9Exception.Drive9> {
                client.patchFileParts(local.toString(), "/r", listOf(0, 1), 100L, 100L, null)
            }
            assertEquals("other", e3.code)
            assertTrue("dirty_parts" in e3.detail, "want dirty_parts error: ${e3.detail}")
        } finally {
            local.deleteIfExists()
        }
    }

    @Test
    fun patchFilePartsUploadsDirtyPartsAndCompletes() = runBlocking {
        val patched = mutableListOf<ByteArray>()
        route("PATCH", "/v1/fs/r") { ex ->
            val body = ex.requestBody.readBytes().toString(StandardCharsets.UTF_8)
            assertTrue("\"dirty_parts\"" in body)
            val response = """{"upload_id":"p1","part_size":4,"upload_parts":[{"number":1,"url":"$baseUrl/patch-part","size":4,"headers":{}}],"copied_parts":[]}"""
                .toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, response.size.toLong())
            ex.responseBody.write(response)
            ex.close()
        }
        route("PUT", "/patch-part") { ex ->
            assertTrue(ex.requestHeaders.getFirst("x-amz-checksum-sha256") != null)
            patched += ex.requestBody.readBytes()
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("POST", "/v1/uploads/p1/complete") { ex ->
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val local = Files.createTempFile("drive9-kotlin-patch", ".bin")
        Files.write(local, "abcdef".toByteArray())
        val client = Drive9Client(baseUrl, "k")
        try {
            client.patchFileParts(local.toString(), "/r", listOf(1), 6, 4)
            assertContentEquals("abcd".toByteArray(), patched.single())
        } finally {
            local.deleteIfExists()
        }
    }

    @Test
    fun patchFilePartsAcceptsChunkedPlanResponse() = runBlocking {
        val patched = mutableListOf<ByteArray>()
        route("PATCH", "/v1/fs/chunked") { ex ->
            ex.requestBody.readBytes()
            val response = """{"upload_id":"pc","part_size":4,"upload_parts":[{"number":1,"url":"$baseUrl/chunked-patch-part","size":4,"headers":{}}],"copied_parts":[]}"""
                .toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.responseHeaders.add("Transfer-Encoding", "chunked")
            ex.sendResponseHeaders(200, 0)
            ex.responseBody.write(response)
            ex.close()
        }
        route("PUT", "/chunked-patch-part") { ex ->
            patched += ex.requestBody.readBytes()
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("POST", "/v1/uploads/pc/complete") { ex ->
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val local = Files.createTempFile("drive9-kotlin-patch-chunked", ".bin")
        Files.write(local, "abcdef".toByteArray())
        val client = Drive9Client(baseUrl, "k")
        try {
            client.patchFileParts(local.toString(), "/chunked", listOf(1), 6, 4)
            assertContentEquals("abcd".toByteArray(), patched.single())
        } finally {
            local.deleteIfExists()
        }
    }

    @Test
    fun streamUploadUsesV2Multipart() = runBlocking {
        val uploaded = mutableListOf<ByteArray>()
        route("POST", "/v2/uploads/initiate") { ex ->
            ex.requestBody.readBytes()
            val body = """{"upload_id":"u1","key":"k","part_size":5,"total_parts":1}""".toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        route("POST", "/v2/uploads/u1/presign") { ex ->
            ex.requestBody.readBytes()
            val body = """{"number":1,"url":"$baseUrl/upload-part","size":5,"headers":{}}""".toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        route("PUT", "/upload-part") { ex ->
            uploaded += ex.requestBody.readBytes()
            ex.responseHeaders.add("etag", "e1")
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("POST", "/v2/uploads/u1/complete") { ex ->
            val body = ex.requestBody.readBytes().toString(StandardCharsets.UTF_8)
            assertTrue("\"number\":1" in body)
            assertTrue("\"etag\":\"e1\"" in body)
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val upload = client.newStreamUpload("/big.bin", 5)
        assertEquals(5L, upload.partSize())
        assertEquals(1, upload.totalParts())
        upload.writePart(1, "hello".toByteArray())
        upload.complete(1, ByteArray(0))
        assertContentEquals("hello".toByteArray(), uploaded.single())
    }

    @Test
    fun downloadFlowReturnsChunks() = runBlocking {
        route("GET", "/v1/fs/big.bin") { ex ->
            val body = "flow-data".toByteArray()
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        val client = Drive9Client(baseUrl, "k")
        val out = ByteArrayOutputStream()
        client.downloadFlow("/big.bin").collect {
            out.write(it)
        }
        assertContentEquals("flow-data".toByteArray(), out.toByteArray())
    }

    @Test
    fun uploadFlowUsesStreamUpload() = runBlocking {
        val uploaded = mutableListOf<ByteArray>()
        route("POST", "/v2/uploads/initiate") { ex ->
            ex.requestBody.readBytes()
            val body = """{"upload_id":"u2","key":"k","part_size":5,"total_parts":1}""".toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        route("POST", "/v2/uploads/u2/presign") { ex ->
            ex.requestBody.readBytes()
            val body = """{"number":1,"url":"$baseUrl/upload-flow-part","size":5,"headers":{}}""".toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        route("PUT", "/upload-flow-part") { ex ->
            uploaded += ex.requestBody.readBytes()
            ex.responseHeaders.add("etag", "e1")
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        route("POST", "/v2/uploads/u2/complete") { ex ->
            ex.requestBody.readBytes()
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        client.uploadFlow("/flow.bin", 5L, flow { emit("he".toByteArray()); emit("llo".toByteArray()) })
        assertContentEquals("hello".toByteArray(), uploaded.single())
    }

    @Test
    fun vaultReadableSecretsAndField() = runBlocking {
        route("GET", "/v1/vault/read") { ex ->
            val body = """{"secrets":["s1"]}""".toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        route("GET", "/v1/vault/read/s1/token") { ex ->
            val body = "secret-value".toByteArray()
            ex.sendResponseHeaders(200, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }
        val client = Drive9Client(baseUrl, "k")
        assertEquals(listOf("s1"), client.vaultListReadableSecrets())
        assertEquals("secret-value", client.vaultReadSecretField("s1", "token"))
    }

    @Test
    fun statusErrorCarriesCode() = runBlocking {
        route("GET", "/v1/fs/missing.txt") { ex ->
            val body = """{"error":"forbidden"}""".toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(403, body.size.toLong())
            ex.responseBody.write(body)
            ex.close()
        }

        val client = Drive9Client(baseUrl, "k")
        val err = assertFailsWith<Drive9Exception.Drive9> {
            client.read("/missing.txt")
        }
        assertEquals("http_status", err.code)
        assertEquals(403, err.statusCode)
        assertEquals("forbidden", err.detail)
    }
}
