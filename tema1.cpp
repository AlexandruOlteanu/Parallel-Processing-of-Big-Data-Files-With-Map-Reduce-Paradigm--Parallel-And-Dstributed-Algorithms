#include <bits/stdc++.h>

using namespace std;

#define NUMBER_OF_ARGUMENTS 4

typedef struct map_data map_data;
typedef struct file_numbers file_numbers;

struct file_numbers {
    string file_name;
    vector<int> numbers;
};

struct map_data {
    vector<file_numbers> files;
    vector<vector<int>> exponent_list;
    int exponent;
};


int string_to_number(string value) {
    int result = 0;
    for (auto c : value) {
        result = result * 10 + c - '0';
    }
    return result;
}

void *create_exponent_lists(void *arg) {

    map_data *data = (map_data *) arg;
    
    int exponent = data->exponent;


    return NULL;
}

void prioritize_files_on_threads(map_data mapper_arguments[], vector<int> numbers[], string files_names[], int mapper_threads_number, int nr_files, int exponent) {
    multiset<pair<int, int>> priority;
    for (int i = 0; i < mapper_threads_number; ++i) {
        priority.insert({0, i});
        mapper_arguments[i].exponent = exponent;
        mapper_arguments[i].exponent_list.resize(exponent + 3);
    }

    for (int i = 0; i < nr_files; ++i) {
        auto first = *priority.begin();
        int new_workload = first.first + numbers[i].size();
        int id = first.second;
        file_numbers file;
        file.file_name = files_names[i];
        file.numbers = numbers[i];
        mapper_arguments[id].files.push_back(file);
        priority.erase(priority.begin());
        priority.insert({new_workload, id});
    }
}


int main(int argc, char *argv[]) {

    if (argc != NUMBER_OF_ARGUMENTS) {
        cout << "Error: Wrong number of Arguments\n";
        return 1;
    }

    ifstream in(argv[3]);
    int nr_files;
    in >> nr_files;
    string files_names[nr_files];
    for (int i = 0; i < nr_files; ++i) {
        in >> files_names[i];
    }

    vector<int> numbers[nr_files];
    for (int i = 0; i < nr_files; ++i) {
        ifstream read(files_names[i]);
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
    map_data mapper_arguments[mapper_threads_number];

    int reducer_threads_number = string_to_number(argv[2]);
    pthread_t reducer_threads[reducer_threads_number];
    // argument reducer_arguments[reducer_threads_number];

    prioritize_files_on_threads(mapper_arguments, numbers, files_names, mapper_threads_number, nr_files, reducer_threads_number);

    int error = 0;
    for (int i = 0; i < mapper_threads_number; ++i) {
        error = pthread_create(&mapper_threads[i], NULL, create_exponent_lists, &mapper_arguments[i]);
        if (error) {
            cout << "Error: Creation of thread with number " << i << " failed!";
            return 1;
        }
    }


    return 0;
}