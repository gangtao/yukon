#include <iostream>

#include "hello.hpp"
#include "datatable.hpp"

#include "rxcpp/rx.hpp"

void sample()
{
    std::string helloJim = generateHelloString("Yukon");
    std::cout << helloJim << std::endl;

    // create a obseverable of Row
    {
        printf("//! [Create flow]\n");
        auto flow = makeDataTableFlow(10, 300);

        flow.subscribe(
            [](Row v)
            { showRow(v); },
            []()
            { std::cout << "Create flow completed!" << std::endl; });
        printf("//! [Create sflow]\n");
    }

    // filter the flow
    {
        printf("//! [Filter flow : where usage > 80 ]\n");
        auto flow = makeDataTableFlow(10, 300).filter([](Row v)
                                                      { return std::get<int>(v["usage"]) > 80; });

        flow.subscribe(
            [](Row v)
            { showRow(v); },
            []()
            { std::cout << "Filter flow completed!" << std::endl; });
        printf("//! [Filter flow ]\n");
    }

    // group by
    {
        printf("//! [Count and Group by tag ]\n");
        auto flow = makeDataTableFlow(10, 300).group_by([](Row v)
                                                        { return std::get<std::string>(v["tag"]); },
                                                        [](Row v)
                                                        { return v; });

        flow.subscribe(
            [](rxcpp::grouped_observable<std::string, Row> g)
            {
                auto key = g.get_key();
                g.count().subscribe(
                    [key](int v)
                    {
                        std::cout << key << " : " << v << std::endl;
                    },
                    [key]()
                    { std::cout << key << " -  group completed!" << std::endl; });
            },
            []()
            { std::cout << "Group by completed!" << std::endl; });
        printf("//! [Group by ]\n");
    }

    // concat
    {
        printf("//! [concat ]\n");
        auto flow1 = makeDataTableFlow(10, 300);
        auto flow2 = makeDataTableFlow(10, 300);

        auto flow = flow1.concat(flow2);

        flow.subscribe(
            [](Row v)
            { showRow(v); },
            []()
            { std::cout << "concat flow completed!" << std::endl; });
        printf("//! [concat ]\n");
    }

    // processing time sliding window
    {
        printf("//! [window ]\n");
        int counter = 0;
        auto flow = makeDataTableFlow(10, 300).window_with_time(std::chrono::milliseconds(900), rxcpp::observe_on_new_thread());

        flow.subscribe(
            [&counter](rxcpp::observable<Row> w)
            {
                int id = counter++;
                printf("[window %d] Create window\n", id);
                w.subscribe(
                    [](Row v)
                    { showRow(v); },
                    []()
                    { std::cout << "concat flow completed!" << std::endl; });
            },
            []()
            { std::cout << "concat flow completed!" << std::endl; });
        printf("//! [window ]\n");
    }
}

int main(int, char **)
{
    sample();
    return 0;
}