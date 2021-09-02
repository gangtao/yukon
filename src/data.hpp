#pragma once

#include <iostream>
#include <map>
#include <variant>
#include <string>
#include <vector>
#include <chrono>

#include "rxcpp/rx.hpp"
#include "cxxfaker/include/Internet.h"

typedef std::string Key;
typedef std::variant<int, float, long, std::string> Value;
typedef std::map<Key, Value> Row;
typedef std::vector<Row> Rows;

long getCurrentTimestamp()
{
    auto t = std::chrono::system_clock::now();
    long ts = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
    return ts;
}

const std::string getValueString(const Value &value)
{
    try
    {
        return std::to_string(std::get<int>(value));
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        return std::to_string(std::get<long>(value));
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        return std::to_string(std::get<float>(value));
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        return std::get<std::string>(value);
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    return std::string("na");
}

void printRow(const std::map<Key, Value> &row)
{
    std::cout << "| ";
    for (auto const &[key, val] : row)
    {
        std::cout << key << " : " << getValueString(val) << " | ";
    }
    std::cout << std::endl;
}

auto makeDataTableFlow(int size, int interval = 1000)
{
    auto flow = rxcpp::observable<>::create<Row>(
        [=](rxcpp::subscriber<Row> s)
        {
            auto generator = cxxfaker::providers::Internet();
            generator.Seed(123);

            int currentSize = 0;
            while (size == -1 || currentSize < size)
            {
                long ts = getCurrentTimestamp();
                auto tags = std::vector<std::string>({"A", "B", "C"});
                auto it = generator.randomElement(tags);
                Row row{
                    {"timestamp", ts},
                    {"usage", generator.randomInt(0, 100)},
                    {"host", generator.IPv4()},
                    {"tag", *it++}};
                s.on_next(row);
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                currentSize++;
            }
            s.on_completed();
        });

    return flow;
}

auto makeWaterMarkFlow(int size, int interval = 1000)
{
    auto flow = rxcpp::observable<>::create<Row>(
        [=](rxcpp::subscriber<Row> s)
        {
            int currentSize = 0;
            while (size == -1 || currentSize < size)
            {
                long ts = getCurrentTimestamp();
                Row row{
                    {"timestamp", ts},
                    {"watermark", 0},
                };
                s.on_next(row);
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                currentSize++;
            }
            s.on_completed();
        });

    return flow;
}

class DataTable
{
private:
    Rows data;

public:
    DataTable(const Rows &rows) : data(rows)
    {
    }

    //constructor with one row
    DataTable(const Row &row) : data(std::vector<Row>({row}))
    {
    }

    void addRow(Row &&row)
    {
        // sort data here
        data.push_back(std::move(row));
    }

    Rows getData() const
    {
        return data;
    }

    void print() const
    {
        for (auto const &row : data)
        {
            printRow(row);
        }
    }
};

// single window data
class DataWindow : public DataTable
{
private:
    long start;
    long end;

public:
    DataWindow(const Rows &rows, const long start, const long end)
        : DataTable(rows), start(start), end(end)
    {
    }

    DataWindow(const Row &row, const long start, const long end)
        : DataTable(row), start(start), end(end)
    {
    }

    void print() const
    {
        std::cout << "window start - " << start << " end - " << end << std::endl;
        DataTable::print();
    }

    long getStart() const
    {
        return start;
    }

    long getEnd() const
    {
        return end;
    }
};

// multiple window data
class DataWindows
{
protected:
    std::map<long, DataWindow> windows;
    long length;
    long firstTimeStamp;

public:
    DataWindows(const long length)
        : length(length), firstTimeStamp(0), windows({})
    {
    }

    DataWindows(const long length, const long firstTimeStamp, const std::map<long, DataWindow> &windows)
        : length(length), firstTimeStamp(firstTimeStamp), windows(windows)
    {
    }

    const DataWindows &addRow(Row &&row)
    {
        long timestamp = std::get<long>(row.at("timestamp"));

        // Check if there is no first row
        if (firstTimeStamp == 0)
        {
            firstTimeStamp = timestamp;
        }

        // find which window the current row belongs to
        long startTimestamp = firstTimeStamp;
        while (timestamp > (startTimestamp + length))
        {
            startTimestamp += length;
        }

        try
        {
            windows.at(startTimestamp).addRow(std::move(row));
        }
        catch (std::out_of_range error)
        {
            DataWindow newWindw(std::move(row), startTimestamp, startTimestamp + length);
            windows.insert({startTimestamp, newWindw});
        }

        return *this;
    }

    virtual void print() const
    {
        for (auto const &[key, val] : windows)
        {
            std::cout << "key : " << key << std::endl;
            val.print();
        }
    }
};

// every time a row is added, check if it should be triggerred,
// after the event is tiggerred, remove it from data window
class DataWindowsWithFixWatermark : public DataWindows
{
private:
    long watermark;
    std::map<long, DataWindow> windows_to_trigger;

public:
    DataWindowsWithFixWatermark(const long length, const long watermark)
        : DataWindows(length), watermark(watermark)
    {
    }

    const DataWindowsWithFixWatermark &addRow(Row &&row)
    {
        DataWindows::addRow(std::move(row));
        long ts = getCurrentTimestamp();
        windows_to_trigger.clear();

        for (auto const &[key, val] : windows)
        {
            auto end = val.getEnd();
            if (ts - end > watermark)
            {
                std::cout << "trigger window with end time : " << end << std::endl;
                windows_to_trigger.insert({key, val});
            }
        }

        // remove triggered window
        for (auto const &[key, val] : windows_to_trigger)
        {
            windows.erase(key);
            std::cout << "remove window with start time : " << key << std::endl;
        }

        return *this;
    }

    bool has_trigger() const
    {
        return windows_to_trigger.size() > 0;
    }

    virtual void print() const
    {
        for (auto const &[key, val] : windows_to_trigger)
        {
            std::cout << "key : " << key << std::endl;
            val.print();
        }
    }
};