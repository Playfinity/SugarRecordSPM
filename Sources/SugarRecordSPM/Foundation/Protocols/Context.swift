import Foundation

public protocol Context: Requestable {
    func fetch<T: Entity>(_ request: FetchRequest<T>) throws -> [T]
    func insert<T: Entity>(_ entity: T) throws
    func new<T: Entity>() throws -> T
    func create<T: Entity>() throws -> T
    func remove<T: Entity>(_ objects: [T]) throws
    func remove<T: Entity>(_ object: T) throws
    func removeAll() throws
}

// MARK: - Extension of Context implementing convenience methods.

public extension Context {
    func create<T: Entity>() throws -> T {
        let instance: T = try new()
        try insert(instance)
        return instance
    }

    func remove<T: Entity>(_ object: T) throws {
        return try remove([object])
    }
}
