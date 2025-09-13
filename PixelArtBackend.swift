import CloudKit
import Foundation

final class CloudKitManager {

    static let shared = CloudKitManager()

    private let container = CKContainer.default()
    private let publicDB: CKDatabase
    private let privateDB: CKDatabase

    private init() {
        self.publicDB = container.publicCloudDatabase
        self.privateDB = container.privateCloudDatabase
    }


    /// Fetches all PixelArt records from the public database (paginated).
    func fetchAllArts() async throws -> [PixelArt] {
        var allArts: [PixelArt] = []
        var cursor: CKQueryOperation.Cursor? = nil

        repeat {
            let (batch, nextCursor) = try await fetchArtBatch(using: cursor)
            allArts.append(contentsOf: batch)
            cursor = nextCursor
        } while cursor != nil

        return allArts
    }

    /// Fetch a single PixelArt by its artId.
    func fetchArt(by artId: String) async throws -> PixelArt {
        let predicate = NSPredicate(format: "artId == %@", artId)
        let query = CKQuery(recordType: "Art", predicate: predicate)

        return try await executeQuery(query, in: publicDB)
    }

    /// Save a new progress record.
    func saveProgress(artId: String, progressJSON: String, percentComplete: Double) async throws {
        let record = CKRecord(recordType: "Progress")
        record["progressId"] = UUID().uuidString
        record["artId"] = artId
        record["pixelsPartialJSON"] = progressJSON
        record["lastUpdated"] = Date()
        record["percentComplete"] = percentComplete

        try await save(record, to: privateDB)
    }

    /// Update an existing progress record by ID.
    func updateProgress(recordID: CKRecord.ID, with progressJSON: String, percentComplete: Double) async throws {
        var record = try await fetch(recordID: recordID, from: privateDB)
        record["pixelsPartialJSON"] = progressJSON
        record["lastUpdated"] = Date()
        record["percentComplete"] = percentComplete

        try await save(record, to: privateDB)
    }

    /// Save a completed art with optional PNG export.
    func saveCompletedArt(artId: String, finalArt: PixelArt, exportedPNGURL: URL? = nil) async throws {
        let record = CKRecord(recordType: "Completed")
        record["completedId"] = UUID().uuidString
        record["artId"] = artId
        record["completedAt"] = Date()

        let jsonURL = try saveToTemporaryJSONFile(finalArt)
        record["pixelsJSONAsset"] = CKAsset(fileURL: jsonURL)

        if let pngURL = exportedPNGURL {
            record["exportPNG"] = CKAsset(fileURL: pngURL)
        }

        try await save(record, to: privateDB)
    }


    /// Fetch a single page of PixelArt records.
    private func fetchArtBatch(using cursor: CKQueryOperation.Cursor?) async throws -> ([PixelArt], CKQueryOperation.Cursor?) {
        return try await withCheckedThrowingContinuation { continuation in
            let operation: CKQueryOperation

            if let cursor = cursor {
                operation = CKQueryOperation(cursor: cursor)
            } else {
                let query = CKQuery(recordType: "Art", predicate: NSPredicate(value: true))
                query.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: true)]
                operation = CKQueryOperation(query: query)
            }

            var results: [PixelArt] = []

            operation.recordFetchedBlock = { record in
                if let art = Self.pixelArt(from: record) {
                    results.append(art)
                }
            }

            operation.queryCompletionBlock = { nextCursor, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: (results, nextCursor))
                }
            }

            self.publicDB.add(operation)
        }
    }

    /// Execute a simple query expecting a single record.
    private func executeQuery(_ query: CKQuery, in database: CKDatabase) async throws -> PixelArt {
        return try await withCheckedThrowingContinuation { continuation in
            let operation = CKQueryOperation(query: query)
            operation.desiredKeys = ["artId", "width", "height", "pixelsJSON", "pixelsAsset", "title"]

            var result: PixelArt?

            operation.recordFetchedBlock = { record in
                result = Self.pixelArt(from: record)
            }

            operation.queryCompletionBlock = { _, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let art = result {
                    continuation.resume(returning: art)
                } else {
                    let error = NSError(domain: "CloudKitManager", code: 404, userInfo: [NSLocalizedDescriptionKey: "Art not found"])
                    continuation.resume(throwing: error)
                }
            }

            database.add(operation)
        }
    }

    /// Save a CKRecord to a given database.
    private func save(_ record: CKRecord, to database: CKDatabase) async throws {
        try await withCheckedThrowingContinuation { continuation in
            database.save(record) { _, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Fetch a CKRecord by ID.
    private func fetch(recordID: CKRecord.ID, from database: CKDatabase) async throws -> CKRecord {
        try await withCheckedThrowingContinuation { continuation in
            database.fetch(withRecordID: recordID) { record, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let record = record {
                    continuation.resume(returning: record)
                } else {
                    let notFoundError = NSError(domain: "CloudKitManager", code: 404, userInfo: [NSLocalizedDescriptionKey: "Record not found"])
                    continuation.resume(throwing: notFoundError)
                }
            }
        }
    }

    /// Write PixelArt to a temp JSON file and return its URL.
    private func saveToTemporaryJSONFile(_ art: PixelArt) throws -> URL {
        let data = try JSONEncoder().encode(art)
        let tmpURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(UUID().uuidString).json")
        try data.write(to: tmpURL)
        return tmpURL
    }

    /// Convert a CKRecord to a PixelArt object.
    private static func pixelArt(from record: CKRecord) -> PixelArt? {
        guard
            let idString = record["artId"] as? String,
            let id = UUID(uuidString: idString),
            let width = record["width"] as? Int,
            let height = record["height"] as? Int
        else {
            return nil
        }

        if let jsonString = record["pixelsJSON"] as? String {
            let data = Data(jsonString.utf8)
            if let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
                return art
            }

            if let jsonObject = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let pixels = jsonObject["pixels"] as? [[String]] {
                return PixelArt(id: id, width: width, height: height, pixels: pixels, numbers: nil, title: record["title"] as? String)
            }
        }

        if let asset = record["pixelsAsset"] as? CKAsset,
           let fileURL = asset.fileURL,
           let data = try? Data(contentsOf: fileURL),
           let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
            return art
        }

        return nil
    }
}
