#include <iostream>

#include "hello.hpp"
#include "datatable.hpp"

#include "rxcpp/rx.hpp"

void sample() {
    std::string helloJim = generateHelloString("Yukon");
    std::cout << helloJim << std::endl;

    // create a obseverable of Row
    {
        printf("//! [Create sample flow]\n");
        auto flow = makeDataTableFlow(-1,300);

        flow.
            subscribe(
                [](Row v){ showRow(v); },
                [](){printf("OnCompleted\n");});
        printf("//! [Create sample flow]\n");
    }
}

int main(int, char **)
{
    sample();
    return 0;
}