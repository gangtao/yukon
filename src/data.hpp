#pragma once

#include <iostream>
#include <map>
#include <variant>
#include <string>
#include <vector>
#include <chrono>
#include <random>

#include "rxcpp/rx.hpp"
#include "cxxfaker/include/Internet.h"

typedef std::string Key;
typedef std::variant<int, float, long, std::string> Value;
typedef std::map<Key, Value> Row;
typedef std::vector<Row> Rows;

std::default_random_engine generator;

long randomDelay(long a, long b)
{
    std::uniform_int_distribution<long> distribution(a, b);
    return distribution(generator);
}

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

auto makeDataTableFlow(int size = 10, int interval = 300, long latency_min = 100l, long latency_max = 200l)
{
    auto period = std::chrono::milliseconds(interval);
    auto values = rxcpp::observable<>::interval(period);

    auto flow = values.take(size).map([=](int v)
                                      {
                                          // TODO : need a glable generator here
                                          auto generator = cxxfaker::providers::Internet();
                                          generator.Seed(rand());
                                          long ts = getCurrentTimestamp();

                                          // simulate delay
                                          long delay = randomDelay(latency_min, latency_max);
                                          auto tags = std::vector<std::string>({"A", "B", "C"});
                                          auto it = generator.randomElement(tags);
                                          Row row{
                                              {"timestamp", ts - delay},
                                              {"usage", generator.randomInt(0, 100)},
                                              {"host", generator.IPv4()},
                                              {"tag", *it++}};
                                          return row;
                                      });

    return flow;
}

auto makeWaterMarkFlow(int size = 10, int interval = 300, long wartermark = 100)
{
    auto period = std::chrono::milliseconds(interval);
    auto values = rxcpp::observable<>::interval(period);

    auto flow = values.take(size).map([=](int v)
                                      {
                                          long ts = getCurrentTimestamp();
                                          Row row{
                                              {"timestamp", ts},
                                              {"watermark", wartermark},
                                          };
                                          return row;
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
        windows_to_trigger.clear();

        long timestamp = std::get<long>(row.at("timestamp"));
        long ts = getCurrentTimestamp();

        // check if the event is a late arrive event
        // only accept event within watermark
        if (ts - timestamp > watermark)
        {
            std::cout << "discard late arrive event : " << timestamp << " current time : " << ts << std::endl;
            return *this;
        }

        DataWindows::addRow(std::move(row));

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

// the data flow contain both real data and dyamically generated watermarks
// the result will be triggerred based on watermarks
class DataWindowsWithDynamicWatermark : public DataWindows
{
private:
    std::map<long, DataWindow> windows_to_trigger;
    long high_watermark = 0l;
    long max_latency = 0l;
    long contorl_buffer = 0l;

public:
    DataWindowsWithDynamicWatermark(const long length)
        : DataWindows(length)
    {
    }

    const DataWindowsWithDynamicWatermark &addRow(Row &&row)
    {
        windows_to_trigger.clear();
        long timestamp = std::get<long>(row.at("timestamp"));
        long ts = getCurrentTimestamp();

        if (row.find("watermark") == row.end())
        {
            // discard late arrive event
            if (timestamp < high_watermark)
            {
                std::cout << "discard late arrive event : " << timestamp << " current time : " << ts << std::endl;
                return *this;
            }

            // data row
            DataWindows::addRow(std::move(row));

            long latency = ts - timestamp;

            if (latency > max_latency)
            {
                max_latency = latency;
            }

            // update watermark
            high_watermark = timestamp - max_latency - contorl_buffer;
        }
        else
        {
            // trigger based on dynamic watermark
            long buffer = std::get<long>(row.at("watermark"));
            contorl_buffer = buffer;

            for (auto const &[key, val] : windows)
            {
                auto end = val.getEnd();
                if (ts - end > max_latency + buffer)
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