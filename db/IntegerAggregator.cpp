#include <db/IntegerAggregator.h>
#include <db/IntField.h>
#include <limits>
#include <memory>
#include <iostream>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    std::vector<Tuple> tuples;
    std::vector<Tuple>::const_iterator it;
    bool isOpen;

public:
    IntegerAggregatorIterator(const std::vector<Tuple>& results)
            : tuples(results), isOpen(false) {}

    void open() override {
        isOpen = true;
        it = tuples.begin();
    }

    bool hasNext() override {
        return isOpen && it != tuples.end();
    }

    Tuple next() override {
        if (!hasNext()) {
            throw std::runtime_error("No more tuples");
        }
        return *it++;
    }

    void rewind() override {
        it = tuples.begin();
    }

    const TupleDesc &getTupleDesc() const override {
        if (tuples.empty()) {
            throw std::runtime_error("No tuples in the iterator");
        }
        return tuples.front().getTupleDesc();
    }

    void close() override {
        isOpen = false;
    }
};


IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield, Aggregator::Op what)
        : gbfield(gbfield), gbfieldtype(gbfieldtype), afield(afield), op(what) {
    switch (op) {
        case Aggregator::Op::MIN:
            aggFunc = [](int a, int b) { return std::min(a, b); };
            break;
        case Aggregator::Op::MAX:
            aggFunc = [](int a, int b) { return std::max(a, b); };
            break;
        case Aggregator::Op::SUM:
            aggFunc = [](int a, int b) { return a + b; };
            break;
        case Aggregator::Op::AVG:
            aggFunc = [](int a, int b) { return a + b; };
            break;
        case Aggregator::Op::COUNT:
            aggFunc = [](int a, int _) { return a + 1; };
            break;
        default:
            throw std::invalid_argument("Unsupported operation");
    }
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    if (!tup) {
        throw std::invalid_argument("Null tuple provided");
    }

    const Field *groupField = nullptr;
    if (gbfield != Aggregator::NO_GROUPING) {
        groupField = &tup->getField(gbfield);
    }

    const Field *aggFieldRaw = &tup->getField(afield);
    const IntField *aggField = dynamic_cast<const IntField *>(aggFieldRaw);
    if (!aggField) {
        throw std::invalid_argument("Expected integer field for aggregation");
    }

    int tupleValue = aggField->getValue();
    if (aggregateData.find(groupField) == aggregateData.end()) {
        count[groupField] = 0;
    }

    int currentValue = aggregateData[groupField];
    int currentCount = count[groupField];
    int newValue = aggFunc(currentValue, tupleValue);
    if (op == Aggregator::Op::AVG) {
        count[groupField] = currentCount + 1;
    }
    aggregateData[groupField] = newValue;
}


DbIterator *IntegerAggregator::iterator() const {
    std::vector<Tuple> results;

    std::vector<std::unique_ptr<Field>> fieldInstances;

    std::vector<Types::Type> types;
    std::vector<std::string> names;
    if (gbfield != Aggregator::NO_GROUPING) {
        types.push_back(gbfieldtype.value_or(Types::INT_TYPE));
        names.push_back("groupField");
    }
    types.push_back(Types::INT_TYPE);
    names.push_back("aggregateValue");

    TupleDesc td(types, names);

    for (const auto& pair : aggregateData) {
        const Field* groupField = pair.first;
        int aggregateValue = pair.second;

        if (op == Aggregator::Op::AVG && count.at(groupField) > 0) {
            aggregateValue /= count.at(groupField);
        }

        Tuple tuple(td);

        fieldInstances.push_back(std::make_unique<IntField>(aggregateValue));
        if (gbfield != Aggregator::NO_GROUPING && groupField) {
            tuple.setField(0, groupField);
            tuple.setField(1, new IntField(aggregateValue));
        } else {
            tuple.setField(0, new IntField(aggregateValue));
        }

        results.push_back(tuple);
    }

    return new IntegerAggregatorIterator(results);
}

