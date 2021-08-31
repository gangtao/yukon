#include <iostream>

#include "hello.hpp"
#include "datatable.hpp"

#include "rxcpp/rx.hpp"

int main(int, char **)
{
    std::string helloJim = generateHelloString("Yukon");
    std::cout << helloJim << std::endl;

    auto values1 = rxcpp::observable<>::range(1, 5);
    values1.subscribe(
        [](int v)
        { printf("Value 1 OnNext: %d\n", v); },
        []()
        { printf("Value 1 OnCompleted\n"); });

    std::array<int, 3> a = {{1, 2, 3}};
    auto values2 = rxcpp::observable<>::iterate(a);
    values2.subscribe(
        [](int v)
        { printf("Value 2 OnNext: %d\n", v); },
        []()
        { printf("Value 2 OnCompleted\n"); });

    // sample of array of variant dictionary
    Rows rows {{{{"a",1}},{{"a",3.2f}},{{"a","8.8f"}}}};  
    auto values3 = rxcpp::observable<>::iterate(rows);
    values3.subscribe(
        [](Row r)
        { showValue(r["a"]); },
        []()
        { printf("Value 3 OnCompleted\n"); });
    

    return 0;
}