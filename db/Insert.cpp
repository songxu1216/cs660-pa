#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    if (hasBeenCalled) {
        return std::nullopt;
    }

    hasBeenCalled = true;
    int count = 0;

    BufferPool &bufferPool = Database::getBufferPool();
    while (childIterator->hasNext()) {
        Tuple tuple = childIterator->next();
        bufferPool.insertTuple(transactionId, tableId, &tuple);
        count++;
    }

    TupleDesc td(std::vector<Types::Type>({Types::INT_TYPE}));
    Tuple countTuple(td);
    IntField intField(count);
    countTuple.setField(0, &intField);


    return countTuple;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId)
: transactionId(t), childIterator(child), tableId(tableId), hasBeenCalled(false)
    {
    // TODO pa3.3: some code goes here
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return childIterator->getTupleDesc();

}

void Insert::open() {
    // TODO pa3.3: some code goes here
    childIterator->open();

}

void Insert::close() {
    // TODO pa3.3: some code goes here
    childIterator->close();

}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    childIterator->rewind();

}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return {childIterator};

}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        childIterator = children[0];
    }
}
