// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "Drive9Mobile",
    platforms: [
        .iOS(.v17),
        .macOS(.v12),
    ],
    products: [
        .library(name: "Drive9Mobile", targets: ["Drive9Mobile"]),
    ],
    targets: [
        .target(
            name: "Drive9Mobile",
            path: "Sources/Drive9Mobile"
        ),
        .testTarget(
            name: "Drive9MobileTests",
            dependencies: ["Drive9Mobile"],
            path: "Tests/Drive9MobileTests"
        ),
    ]
)
