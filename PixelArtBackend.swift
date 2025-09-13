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

    // MARK: - Fetch all arts (supports pagination)
    func fetchAllArts() async throws -> [PixelArt] {
        var allResults = [PixelArt]()
        var cursor: CKQueryOperation.Cursor? = nil

        repeat {
            let (results, nextCursor) = try await fetchArtsBatch(cursor: cursor)
            allResults.append(contentsOf: results)
            cursor = nextCursor
        } while cursor != nil

        return allResults
    }

    // Helper to fetch a batch (or first batch if cursor is nil)
    private func fetchArtsBatch(cursor: CKQueryOperation.Cursor?) async throws -> ([PixelArt], CKQueryOperation.Cursor?) {
        return try await withCheckedThrowingContinuation { continuation in
            let operation: CKQueryOperation
            if let cursor = cursor {
                operation = CKQueryOperation(cursor: cursor)
            } else {
                let query = CKQuery(recordType: "Art", predicate: NSPredicate(value: true))
                query.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: true)]
                operation = CKQueryOperation(query: query)
            }

            var results = [PixelArt]()
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

    // MARK: - Fetch single art by artId
    func fetchArt(artId: String) async throws -> PixelArt {
        return try await withCheckedThrowingContinuation { continuation in
            let predicate = NSPredicate(format: "artId == %@", artId)
            let query = CKQuery(recordType: "Art", predicate: predicate)
            let operation = CKQueryOperation(query: query)
            operation.desiredKeys = ["artId", "width", "height", "pixelsJSON", "pixelsAsset", "title"]

            var foundArt: PixelArt?

            operation.recordFetchedBlock = { record in
                foundArt = Self.pixelArt(from: record)
            }

            operation.queryCompletionBlock = { _, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let art = foundArt {
                    continuation.resume(returning: art)
                } else {
                    let error = NSError(domain: "CloudKitManager", code: 404, userInfo: [NSLocalizedDescriptionKey: "Art not found"])
                    continuation.resume(throwing: error)
                }
            }

            self.publicDB.add(operation)
        }
    }

    // MARK: - Save progress
    func saveProgress(artId: String, progressJSON: String, percent: Double) async throws {
        let record = CKRecord(recordType: "Progress")
        record["progressId"] = UUID().uuidString
        record["artId"] = artId
        record["pixelsPartialJSON"] = progressJSON
        record["lastUpdated"] = Date()
        record["percentComplete"] = percent

        try await saveRecord(record, database: privateDB)
    }

    // MARK: - Update existing progress
    func updateProgress(recordID: CKRecord.ID, progressJSON: String, percent: Double) async throws {
        let record = try await fetchRecord(recordID: recordID, database: privateDB)
        record["pixelsPartialJSON"] = progressJSON
        record["lastUpdated"] = Date()
        record["percentComplete"] = percent

        try await saveRecord(record, database: privateDB)
    }

    // MARK: - Save completed art
    func saveCompleted(artId: String, finalArt: PixelArt, exportedPNG: URL?) async throws {
        let record = CKRecord(recordType: "Completed")
        record["completedId"] = UUID().uuidString
        record["artId"] = artId
        record["completedAt"] = Date()

        let encoder = JSONEncoder()
        let data = try encoder.encode(finalArt)
        let tmpURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(UUID().uuidString).json")
        try data.write(to: tmpURL)
        record["pixelsJSONAsset"] = CKAsset(fileURL: tmpURL)

        if let pngURL = exportedPNG {
            record["exportPNG"] = CKAsset(fileURL: pngURL)
        }

        try await saveRecord(record, database: privateDB)
    }

    // MARK: - Helper functions for async CK operations

    private func saveRecord(_ record: CKRecord, database: CKDatabase) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            database.save(record) { savedRecord, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    private func fetchRecord(recordID: CKRecord.ID, database: CKDatabase) async throws -> CKRecord {
        return try await withCheckedThrowingContinuation { continuation in
            database.fetch(withRecordID: recordID) { record, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let record = record {
                    continuation.resume(returning: record)
                } else {
                    let error = NSError(domain: "CloudKitManager", code: 404, userInfo: [NSLocalizedDescriptionKey: "Record not found"])
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    // MARK: - Helper: convert CKRecord -> PixelArt
    private static func pixelArt(from record: CKRecord) -> PixelArt? {
        guard let idString = record["artId"] as? String,
              let id = UUID(uuidString: idString),
              let width = record["width"] as? Int,
              let height = record["height"] as? Int else {
            return nil
        }

        if let pixelsJSON = record["pixelsJSON"] as? String {
            let data = Data(pixelsJSON.utf8)
            if let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
                return art
            }

            if let decoded = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let pixels = decoded["pixels"] as? [[String]] {
                return PixelArt(id: id, width: width, height: height, pixels: pixels, numbers: nil, title: record["title"] as? String)
            }
        }

        if let pixelsAsset = record["pixelsAsset"] as? CKAsset,
           let fileURL = pixelsAsset.fileURL,
           let data = try? Data(contentsOf: fileURL),
           let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
            return art
        }

        return nil
    }
}
