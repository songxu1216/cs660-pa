#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child)
: transactionId(t), childIterator(child), hasBeenCalled(false), tupleDesc(std::vector<Types::Type>({Types::INT_TYPE}))
{
    // TODO pa3.3: some code goes here
}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return tupleDesc;

}

void Delete::open() {
    // TODO pa3.3: some code goes here
    childIterator->open();

}

void Delete::close() {
    // TODO pa3.3: some code goes here
    childIterator->close();

}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    childIterator->rewind();

}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    return {childIterator};

}

void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        childIterator = children[0];
    }
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
    if (hasBeenCalled) {
        return std::nullopt;
    }

    hasBeenCalled = true;
    int count = 0;

    BufferPool &bufferPool = Database::getBufferPool();
    while (childIterator->hasNext()) {
        Tuple tuple = childIterator->next();
        bufferPool.deleteTuple(transactionId, &tuple);
        count++;
    }

    // Create a tuple for the count
    Tuple countTuple(tupleDesc);
    IntField intField(count);
    countTuple.setField(0, &intField);

    return countTuple;
}
