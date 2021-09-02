#include <iostream>

#include "hello.hpp"
#include "data.hpp"

#include "rxcpp/rx.hpp"

typedef void (*FnPtr)();

void demo_create()
{
    // create a obseverable of Row
    printf("//! [create flow]\n");
    auto flow = makeDataTableFlow(10, 300);

    flow.subscribe(
        [](Row v)
        { printRow(v); },
        []()
        { std::cout << "Create flow completed!" << std::endl; });
    printf("//! [create flow]\n\n");
}

void demo_filter()
{
    printf("//! [filter flow : where usage > 80 ]\n");
    auto flow = makeDataTableFlow(10, 300).filter([](Row v)
                                                  { return std::get<int>(v["usage"]) > 80; });

    flow.subscribe(
        [](Row v)
        { printRow(v); },
        []()
        { std::cout << "filter flow completed!" << std::endl; });
    printf("//! [filter flow ]\n\n");
}

void demo_max()
{
    printf("//! [max usage: where state is required ]\n");
    auto local_state_max = 0;
    auto flow = makeDataTableFlow(10, 300).map([&local_state_max](Row v)
                                               {
                                                   auto usage = std::get<int>(v["usage"]);
                                                   if (usage > local_state_max)
                                                   {
                                                       local_state_max = usage;
                                                       return usage;
                                                   }
                                                   else
                                                   {
                                                       return local_state_max;
                                                   }
                                               });

    flow.subscribe(
        [](int v)
        { std::cout << "max usage is: " << v << std::endl; },
        []()
        { std::cout << "filter flow completed!" << std::endl; });
    printf("//! [filter flow ]\n\n");
}

void demo_groupby()
{
    printf("//! [count and group by tag ]\n");
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
    printf("//! [group by ]\n\n");
}

void demo_concat()
{
    printf("//! [concat ]\n");
    auto flow1 = makeDataTableFlow(10, 300);
    auto flow2 = makeDataTableFlow(10, 300);

    auto flow = flow1.concat(flow2);

    flow.subscribe(
        [](Row v)
        { printRow(v); },
        []()
        { std::cout << "concat flow completed!" << std::endl; });
    printf("//! [concat ]\n\n");
}

void demo_window_sliding()
{
    printf("//! [window with processing time]\n");
    int counter = 0;
    auto flow = makeDataTableFlow(10, 300).window_with_time(std::chrono::milliseconds(900), rxcpp::observe_on_new_thread());

    flow.subscribe(
        [&counter](rxcpp::observable<Row> w)
        {
            int id = counter++;
            printf("[window %d] Create window\n", id);
            w.subscribe(
                [](Row v)
                { printRow(v); },
                [id]()
                { std::cout << "window " << id << " flow completed!" << std::endl; });
        },
        []()
        { std::cout << "window flow completed!" << std::endl; });
    printf("//! [window with processing time ]\n\n");
}

void demo_eventtime_window_sliding()
{
    printf("//! [window with event time]\n");
    DataWindows local_window_state(1500);
    Rows local_rows;
    auto flow = makeDataTableFlow(10, 300).map([&](Row v)
                                               {
                                                   local_window_state.addRow(std::move(v));
                                                   local_rows.push_back(v);
                                                   return local_window_state;
                                               });

    flow.subscribe(
        [](DataWindows windows)
        {
            std::cout << " event trigger: " << std::endl; 
            windows.print(); 
            std::cout << " event trigger completed!" << std::endl;
        },
        []()
        { std::cout << "window with event time completed!" << std::endl << std::endl; });
    printf("//! [window with event time ]\n\n");
}

void wait()
{
    do
    {
        std::cout << '\n' << "Press enter to continue..." ;
    } while (std::cin.get() != '\n');
}

void demo(const std::map<std::string, FnPtr> &fmap)
{
    for (auto const &[key, val] : fmap)
    {
        std::cout << "demo to run : " << key << std::endl;
        wait();
        val();
    }
}

int main(int, char **)
{
    std::string helloJim = generateHelloString("Yukon");
    std::cout << helloJim << std::endl;

    std::map<std::string, FnPtr> functionMap;

    //functionMap["0_demo_create"] = demo_create;
    //functionMap["1_demo_filter"] = demo_filter;
    //functionMap["2_demo_groupby"] = demo_groupby;
    //functionMap["3_demo_max"] = demo_max;
    //functionMap["4_demo_concat"] = demo_concat;
    //functionMap["5_demo_window_sliding"] = demo_window_sliding;
    functionMap["6_demo_eventtime_window_sliding"] = demo_eventtime_window_sliding;

    demo(functionMap);

    return 0;
}