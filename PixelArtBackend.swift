import CloudKit
import Foundation
import UIKit

final class CloudKitManager {
    static let shared = CloudKitManager()
    private let container = CKContainer.default()
    private let publicDB: CKDatabase
    private let privateDB: CKDatabase

    private init() {
        self.publicDB = container.publicCloudDatabase
        self.privateDB = container.privateCloudDatabase
    }

    //  Fetch all arts (public)
    func fetchAllArts(completion: @escaping (Result<[PixelArt], Error>) -> Void) {
        let query = CKQuery(recordType: "Art", predicate: NSPredicate(value: true))
        let sort = NSSortDescriptor(key: "creationDate", ascending: true)
        query.sortDescriptors = [sort]

        let operation = CKQueryOperation(query: query)
        var results: [PixelArt] = []

        operation.recordFetchedBlock = { record in
            if let art = Self.pixelArt(from: record) {
                results.append(art)
            }
        }

        operation.queryCompletionBlock = { _, error in
            if let e = error { completion(.failure(e)); return }
            completion(.success(results))
        }
        publicDB.add(operation)
    }

    //  Fetch single art by artId
    func fetchArt(artId: String, completion: @escaping (Result<PixelArt, Error>) -> Void) {
        let predicate = NSPredicate(format: "artId == %@", artId)
        let query = CKQuery(recordType: "Art", predicate: predicate)
        let operation = CKQueryOperation(query: query)
        operation.desiredKeys = ["artId", "width", "height", "pixelsJSON", "numbersJSON", "title", "author"]

        var found: PixelArt?
        operation.recordFetchedBlock = { record in
            found = Self.pixelArt(from: record)
        }
        operation.queryCompletionBlock = { _, error in
            if let e = error { completion(.failure(e)); return }
            if let art = found { completion(.success(art)) }
            else { completion(.failure(NSError(domain: "CloudKit", code: 404, userInfo: [NSLocalizedDescriptionKey: "Art not found"]))) }
        }
        publicDB.add(operation)
    }

    // MARK: - Save progress (private)
    /// store either a compact "delta" JSON or full pixels JSON in 'pixelsPartialJSON' field
    func saveProgress(artId: String, progressJSON: String, percent: Double, completion: @escaping (Result<Void, Error>) -> Void) {
        // you can make progressId deterministic: e.g. userID+artId
        let record = CKRecord(recordType: "Progress")
        record["progressId"] = UUID().uuidString
        record["artId"] = artId
        record["pixelsPartialJSON"] = progressJSON
        record["lastUpdated"] = Date()
        record["percentComplete"] = percent

        privateDB.save(record) { _, error in
            DispatchQueue.main.async {
                if let e = error { completion(.failure(e)); return }
                completion(.success(()))
            }
        }
    }

    // MARK: - Update existing progress (use recordID)
    func updateProgress(recordID: CKRecord.ID, progressJSON: String, percent: Double, completion: @escaping (Result<Void, Error>) -> Void) {
        privateDB.fetch(withRecordID: recordID) { record, error in
            if let e = error { DispatchQueue.main.async { completion(.failure(e)) }; return }
            guard let r = record else { DispatchQueue.main.async { completion(.failure(NSError())) }; return }
            r["pixelsPartialJSON"] = progressJSON
            r["lastUpdated"] = Date()
            r["percentComplete"] = percent

            self.privateDB.save(r) { _, err in
                DispatchQueue.main.async {
                    if let e = err { completion(.failure(e)); return }
                    completion(.success(()))
                }
            }
        }
    }

    //  Save completed art .
    func saveCompleted(artId: String, finalArt: PixelArt, exportedPNG: URL?, completion: @escaping (Result<Void, Error>) -> Void) {
        let record = CKRecord(recordType: "Completed")
        record["completedId"] = UUID().uuidString
        record["artId"] = artId

        // Save JSON as asset if big
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = []
            let data = try encoder.encode(finalArt)
            // write to temp file
            let tmpURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(UUID().uuidString).json")
            try data.write(to: tmpURL)
            let asset = CKAsset(fileURL: tmpURL)
            record["pixelsJSONAsset"] = asset
        } catch {
            DispatchQueue.main.async { completion(.failure(error)); return }
        }

        if let pngURL = exportedPNG {
            record["exportPNG"] = CKAsset(fileURL: pngURL)
        }
        record["completedAt"] = Date()

        privateDB.save(record) { _, error in
            DispatchQueue.main.async {
                if let e = error { completion(.failure(e)); return }
                completion(.success(()))
            }
        }
    }

    // convert CKRecord -> PixelArt
    private static func pixelArt(from record: CKRecord) -> PixelArt? {
        guard let idString = record["artId"] as? String,
              let id = UUID(uuidString: idString),
              let width = record["width"] as? Int,
              let height = record["height"] as? Int
        else { return nil }

        // try JSON string
        if let pixelsJSON = record["pixelsJSON"] as? String {
            let data = Data(pixelsJSON.utf8)
            if let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
                return art
            }
            // fallback: parse partial structure
            if let decoded = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let pixels = decoded["pixels"] as? [[String]] {
                return PixelArt(id: id, width: width, height: height, pixels: pixels, numbers: nil, title: record["title"] as? String, author: record["author"] as? String)
            }
        }

        // try assets
        if let pixelsAsset = record["pixelsAsset"] as? CKAsset,
           let fileURL = pixelsAsset.fileURL,
           let data = try? Data(contentsOf: fileURL),
           let art = try? JSONDecoder().decode(PixelArt.self, from: data) {
            return art
        }

        return nil
    }

} // end
