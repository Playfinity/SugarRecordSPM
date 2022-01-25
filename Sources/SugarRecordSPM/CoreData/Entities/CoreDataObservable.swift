import CoreData
import Foundation

@available(OSX 10.12, *)
public class CoreDataObservable<T: NSManagedObject>: RequestObservable<T>, NSFetchedResultsControllerDelegate {
    // MARK: - Attributes

    internal let fetchRequest: NSFetchRequest<NSFetchRequestResult>
    internal var observer: ((ObservableChange<T>) -> Void)?
    internal let fetchedResultsController: NSFetchedResultsController<NSFetchRequestResult>
    private var batchChanges: [CoreDataChange<T>] = []

    // MARK: - Init

    internal init(request: FetchRequest<T>, context: NSManagedObjectContext) {
        let fetchRequest: NSFetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: T.entityName)
        if let predicate = request.predicate {
            fetchRequest.predicate = predicate
        }
        if let sortDescriptor = request.sortDescriptor {
            fetchRequest.sortDescriptors = [sortDescriptor]
        }
        fetchRequest.fetchBatchSize = 0
        self.fetchRequest = fetchRequest
        fetchedResultsController = NSFetchedResultsController(fetchRequest: fetchRequest, managedObjectContext: context, sectionNameKeyPath: nil, cacheName: nil)
        super.init(request: request)
        fetchedResultsController.delegate = self
    }

    // MARK: - Observable

    override public func observe(_ closure: @escaping (ObservableChange<T>) -> Void) {
        assert(observer == nil, "Observable can be observed only once")
        let initial = try! fetchedResultsController.managedObjectContext.fetch(fetchRequest) as! [T]
        closure(ObservableChange.initial(initial))
        observer = closure
        _ = try? fetchedResultsController.performFetch()
    }

    // MARK: - Dipose Method

    override func dispose() {
        fetchedResultsController.delegate = nil
    }

    // MARK: - NSFetchedResultsControllerDelegate

    public func controller(_ controller: NSFetchedResultsController<NSFetchRequestResult>, didChange anObject: Any, at indexPath: IndexPath?, for type: NSFetchedResultsChangeType, newIndexPath: IndexPath?) {
        let index: Int? = indexPath?[1]
        let newIndex: Int? = newIndexPath?[1]

        switch type {
        case .delete:
            batchChanges.append(.delete(index!, anObject as! T))
        case .insert:
            batchChanges.append(.insert(newIndex!, anObject as! T))
        case .update:
            batchChanges.append(.update(index!, anObject as! T))
        default: break
        }
    }

    public func controllerWillChangeContent(_ controller: NSFetchedResultsController<NSFetchRequestResult>) {
        batchChanges = []
    }

    public func controllerDidChangeContent(_ controller: NSFetchedResultsController<NSFetchRequestResult>) {
        let deleted = batchChanges.filter { $0.isDeletion }.map { $0.index() }
        let inserted = batchChanges.filter { $0.isInsertion }.map { (index: $0.index(), element: $0.object()) }
        let updated = batchChanges.filter { $0.isUpdate }.map { (index: $0.index(), element: $0.object()) }
        observer?(ObservableChange.update(deletions: deleted, insertions: inserted, modifications: updated))
    }
}
