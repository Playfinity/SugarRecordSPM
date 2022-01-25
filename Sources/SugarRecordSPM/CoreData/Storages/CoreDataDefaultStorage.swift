import CoreData
import Foundation

public class CoreDataDefaultStorage: Storage {
    // MARK: - Attributes

    internal let store: CoreDataStore
    internal var objectModel: NSManagedObjectModel!
    internal var persistentStore: NSPersistentStore!
    internal var persistentStoreCoordinator: NSPersistentStoreCoordinator!
    internal var rootSavingContext: NSManagedObjectContext!

    // MARK: - Storage conformance

    public var description: String {
        return "CoreDataDefaultStorage"
    }

    public var type: StorageType = .coreData
    public var mainContext: Context!
    private var _saveContext: Context!
    public var saveContext: Context! {
        if let context = _saveContext {
            return context
        }
        let _context = cdContext(withParent: .context(rootSavingContext), concurrencyType: .privateQueueConcurrencyType, inMemory: false)
        _context.observe(inMainThread: true) { [weak self] (notification) -> Void in
            (self?.mainContext as? NSManagedObjectContext)?.mergeChanges(fromContextDidSave: notification as Notification)
        }
        _saveContext = _context
        return _context
    }

    public var memoryContext: Context! {
        let _context = cdContext(withParent: .context(rootSavingContext), concurrencyType: .privateQueueConcurrencyType, inMemory: true)
        return _context
    }

    public func operation<T>(_ operation: @escaping (_ context: Context, _ save: @escaping () -> Void) throws -> T) throws -> T {
        let context: NSManagedObjectContext = saveContext as! NSManagedObjectContext
        var _error: Error!

        var returnedObject: T!
        context.performAndWait {
            do {
                returnedObject = try operation(context, { () -> Void in
                    do {
                        try context.save()
                    } catch {
                        _error = error
                    }
                    self.rootSavingContext.performAndWait({
                        if self.rootSavingContext.hasChanges {
                            do {
                                try self.rootSavingContext.save()
                            } catch {
                                _error = error
                            }
                        }
                    })
                })
            } catch {
                _error = error
            }
        }
        if let error = _error {
            throw error
        }

        return returnedObject
    }

    public func backgroundOperation(_ operation: @escaping (_ context: Context, _ save: @escaping () -> Void) -> Void, completion: @escaping (Error?) -> Void) {
        let context: NSManagedObjectContext = saveContext as! NSManagedObjectContext
        var _error: Error!
        context.perform {
            operation(context, { () -> Void in
                do {
                    try context.save()
                } catch {
                    _error = error
                }
                self.rootSavingContext.perform {
                    if self.rootSavingContext.hasChanges {
                        do {
                            try self.rootSavingContext.save()
                        } catch {
                            _error = error
                        }
                    }
                    completion(_error)
                }
            })
        }
    }

    public func removeStore() throws {
        try FileManager.default.removeItem(at: store.path() as URL)
        _ = try? FileManager.default.removeItem(atPath: "\(store.path().absoluteString)-shm")
        _ = try? FileManager.default.removeItem(atPath: "\(store.path().absoluteString)-wal")
    }

    // MARK: - Init

    public convenience init(store: CoreDataStore, model: CoreDataObjectModel, migrate: Bool = true) throws {
        try self.init(store: store, model: model, migrate: migrate, versionController: VersionController())
    }

    internal init(store: CoreDataStore, model: CoreDataObjectModel, migrate: Bool = true, versionController: VersionController) throws {
        self.store = store
        objectModel = model.model()!
        persistentStoreCoordinator = NSPersistentStoreCoordinator(managedObjectModel: objectModel)
        persistentStore = try cdInitializeStore(store: store, storeCoordinator: persistentStoreCoordinator, migrate: migrate)
        rootSavingContext = cdContext(withParent: .coordinator(persistentStoreCoordinator), concurrencyType: .privateQueueConcurrencyType, inMemory: false)
        mainContext = cdContext(withParent: .context(rootSavingContext), concurrencyType: .mainQueueConcurrencyType, inMemory: false)
        #if DEBUG
            versionController.check()
        #endif
    }

    // MARK: - Public

    @available(OSX 10.12, *)
    public func observable<T: NSManagedObject>(request: FetchRequest<T>) -> RequestObservable<T> {
        return CoreDataObservable(request: request, context: mainContext as! NSManagedObjectContext)
    }
}

// MARK: - Internal

internal func cdContext(withParent parent: CoreDataContextParent?, concurrencyType: NSManagedObjectContextConcurrencyType, inMemory: Bool) -> NSManagedObjectContext {
    var context: NSManagedObjectContext?
    if inMemory {
        context = NSManagedObjectMemoryContext(concurrencyType: concurrencyType)
    } else {
        context = NSManagedObjectContext(concurrencyType: concurrencyType)
    }
    if let parent = parent {
        switch parent {
        case let .context(parentContext):
            context!.parent = parentContext
        case let .coordinator(storeCoordinator):
            context!.persistentStoreCoordinator = storeCoordinator
        }
    }
    context!.observeToGetPermanentIDsBeforeSaving()
    return context!
}

internal func cdInitializeStore(store: CoreDataStore, storeCoordinator: NSPersistentStoreCoordinator, migrate: Bool) throws -> NSPersistentStore {
    try cdCreateStoreParentPathIfNeeded(store: store)
    let options = migrate ? CoreDataOptions.migration : CoreDataOptions.basic
    return try cdAddPersistentStore(store: store, storeCoordinator: storeCoordinator, options: options.dict())
}

internal func cdCreateStoreParentPathIfNeeded(store: CoreDataStore) throws {
    let databaseParentPath = store.path().deletingLastPathComponent()
    try FileManager.default.createDirectory(at: databaseParentPath, withIntermediateDirectories: true, attributes: nil)
}

internal func cdAddPersistentStore(store: CoreDataStore, storeCoordinator: NSPersistentStoreCoordinator, options: [String: AnyObject]) throws -> NSPersistentStore {
    func addStore(_ store: CoreDataStore, _ storeCoordinator: NSPersistentStoreCoordinator, _ options: [String: AnyObject], _ cleanAndRetryIfMigrationFails: Bool) throws -> NSPersistentStore {
        var persistentStore: NSPersistentStore?
        var error: NSError?
        storeCoordinator.performAndWait({ () -> Void in
            do {
                persistentStore = try storeCoordinator.addPersistentStore(ofType: NSSQLiteStoreType, configurationName: nil, at: store.path() as URL, options: options)
            } catch let _error as NSError {
                error = _error
            }
        })
        if let error = error {
            let isMigrationError = error.code == NSPersistentStoreIncompatibleVersionHashError || error.code == NSMigrationMissingSourceModelError
            if isMigrationError && cleanAndRetryIfMigrationFails {
                _ = try? cdCleanStoreFilesAfterFailedMigration(store: store)
                return try addStore(store, storeCoordinator, options, false)
            } else {
                throw error
            }
        } else if let persistentStore = persistentStore {
            return persistentStore
        }
        throw CoreDataError.persistenceStoreInitialization
    }
    return try addStore(store, storeCoordinator, options, true)
}

internal func cdCleanStoreFilesAfterFailedMigration(store: CoreDataStore) throws {
    let rawUrl: String = store.path().absoluteString
    let shmSidecar: NSURL = NSURL(string: rawUrl.appending("-shm"))!
    let walSidecar: NSURL = NSURL(string: rawUrl.appending("-wal"))!
    try FileManager.default.removeItem(at: store.path() as URL)
    try FileManager.default.removeItem(at: shmSidecar as URL)
    try FileManager.default.removeItem(at: walSidecar as URL)
}
