#include <bits/stdc++.h>

using namespace std;

#define NUMBER_OF_ARGUMENTS 4

typedef struct argument argument;

struct argument {

};


int string_to_number(string value) {
    int result = 0;
    for (auto c : value) {
        result = result * 10 + c - '0';
    }
    return result;
}

void *create_exponent_lists(void *arg) {





    return NULL;
}


int main(int argc, char *argv[]) {

    if (argc != NUMBER_OF_ARGUMENTS) {
        cout << "Error: Wrong number of Arguments\n";
        return 1;
    }

    ifstream in(argv[3]);
    int nr_files;
    in >> nr_files;
    string files[nr_files];
    for (int i = 0; i < nr_files; ++i) {
        in >> files[i];
    }

    vector<int> numbers[nr_files];
    for (int i = 0; i < nr_files; ++i) {
        ifstream read(files[i]);
        int nr_numbers;
        read >> nr_numbers;
        for (int j = 1; j <= nr_numbers; ++j) {
            int number;
            read >> number;
            numbers[i].push_back(number);
        }
    }
    
    int mapper_threads_number = string_to_number(argv[1]);
    pthread_t mapper_threads[mapper_threads_number];
    argument mapper_arguments[mapper_threads_number];
    
    int reducer_threads_number = string_to_number(argv[2]);
    pthread_t reducer_threads[reducer_threads_number];
    argument reducer_arguments[reducer_threads_number];

    int error = 0;
    for (int i = 0; i < mapper_threads_number; ++i) {

        // mapper_arguments[i].

        error = pthread_create(&mapper_threads[i], NULL, create_exponent_lists, &mapper_arguments[i]);
        if (error) {
            cout << "Error: Creation of thread with number " << i << " failed!";
            return 1;
        }
    }


    return 0;
}