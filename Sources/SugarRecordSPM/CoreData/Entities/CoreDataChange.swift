import Foundation

internal enum CoreDataChange<T> {
    case update(Int, T)
    case delete(Int, T)
    case insert(Int, T)

    internal func object() -> T {
        switch self {
        case let .update(_, object): return object
        case let .delete(_, object): return object
        case let .insert(_, object): return object
        }
    }

    internal func index() -> Int {
        switch self {
        case let .update(index, _): return index
        case let .delete(index, _): return index
        case let .insert(index, _): return index
        }
    }

    internal var isDeletion: Bool {
        switch self {
        case .delete: return true
        default: return false
        }
    }

    internal var isUpdate: Bool {
        switch self {
        case .update: return true
        default: return false
        }
    }

    internal var isInsertion: Bool {
        switch self {
        case .insert: return true
        default: return false
        }
    }
}
