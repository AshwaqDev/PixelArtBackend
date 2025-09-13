

import Foundation

struct PixelArt: Codable {
    let id: UUID
    let width: Int
    let height: Int
    let pixels: [[String]]   // color hex strings like "0xff463f18"
    let numbers: [[Int]]?
    let title: String?
    let author: String?
}
