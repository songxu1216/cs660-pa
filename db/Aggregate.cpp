#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
    if (!openFlag) {
        throw std::runtime_error("Aggregate iterator not open");
    }

    if (aggIterator && aggIterator->hasNext()) {
        return aggIterator->next();
    }
    return std::nullopt;
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop)
        : child(child), afield(afield), gfield(gfield), aop(aop), aggregator(nullptr), aggIterator(nullptr), openFlag(false) {

    // Check if there is grouping
    std::optional<Types::Type> gbfieldtype;
    if (gfield != Aggregator::NO_GROUPING) {
        gbfieldtype = child->getTupleDesc().getFieldType(gfield);
    }

    // Determine the type of afield and create the appropriate aggregator
    Types::Type afieldType = child->getTupleDesc().getFieldType(afield);
    if (afieldType == Types::INT_TYPE) {
        aggregator = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
    } else if (afieldType == Types::STRING_TYPE) {
        if (aop != Aggregator::Op::COUNT) {
            throw std::invalid_argument("Unsupported operation for StringAggregator");
        }
        aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
    } else {
        throw std::invalid_argument("Unsupported field type for aggregation");
    }
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return gfield;
}

std::string Aggregate::groupFieldName() const {
    // TODO pa3.2: some code goes here
    return (gfield == Aggregator::NO_GROUPING) ? "" : child->getTupleDesc().getFieldName(gfield);

}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;

}

std::string Aggregate::aggregateFieldName() const {
    // TODO pa3.2: some code goes here
    return child->getTupleDesc().getFieldName(afield);

}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;

}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    child->open();
    while (child->hasNext()) {
        Tuple t = child->next();
        aggregator->mergeTupleIntoGroup(&t);
    }
    aggIterator = aggregator->iterator();
    aggIterator->open();
    openFlag = true;
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    close();
    open();

}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    if (gfield == Aggregator::NO_GROUPING) {
        std::vector<Types::Type> types = {child->getTupleDesc().getFieldType(afield)};
        std::vector<std::string> names = {aggregateFieldName()};
        return TupleDesc(types, names);
    } else {
        std::vector<Types::Type> types = {child->getTupleDesc().getFieldType(gfield), child->getTupleDesc().getFieldType(afield)};
        std::vector<std::string> names = {groupFieldName(), aggregateFieldName()};
        return TupleDesc(types, names);
    }
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    if (aggIterator) {
        aggIterator->close();
    }
    child->close();
    openFlag = false;
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    return std::vector<DbIterator *>{child};

}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}
