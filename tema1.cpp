#include <bits/stdc++.h>

using namespace std;

#define NUMBER_OF_ARGUMENTS 4

typedef struct map_data map_data;
typedef struct file_numbers file_numbers;

struct file_numbers {
    string file_name;
    vector<long long> numbers;
};

struct map_data {
    vector<file_numbers> files;
    vector<vector<long long>> exponent_list;
    int exponent;
    int id;
    pthread_barrier_t *barrier;
};

struct reducer_data {
    vector<vector<long long>> lists;
    int exponent_to_check;
    int unique_values;
    int mappers_number;
    pthread_barrier_t *barrier;
    map_data *mapper_arguments;
};


int string_to_number(string value) {
    int result = 0;
    for (auto c : value) {
        result = result * 10 + c - '0';
    }
    return result;
}

string number_to_string(int value) {
    string result = "";
    if (value == 0) {
        result = "0";
        return result;
    }
    while (value) {
        result = char(value % 10 + '0') + result;
        value /= 10;
    }
    return result;
}

short check(long long a, long long b, long long c) {
    long long result = 1;
    while (b) {
        if (b & 1) {
            result *= a;
        }
        b /= 2;
        a *= a;
        if (result > c) {
            return 1;
        }
    }
    if (result == c) {
        return 0;
    }

    return -1;
}

void *create_exponent_lists(void *arg) {

    map_data *data = (map_data *) arg;
    
    int exponent = data->exponent;
    for (auto file : data->files) {
        for (auto number : file.numbers) {
            if (number == 1) {
                for (long long i = 2; i <= exponent; ++i) {
                    data->exponent_list[i].push_back(number);
                }
                continue;
            }
            for (long long i = 2; i <= exponent; ++i) {
                if (check(2, i, number) > 0) {
                    break;
                }
                long long left = 1, right = sqrt(number);
                bool ok = 0;
                while (left <= right) {
                    long long middle = left + (right - left) / 2;
                    short check_result = check(middle, i, number);
                    if (check_result == 0) {
                        ok = 1;
                        left = right + 1;
                        continue;
                    }
                    if (check_result < 0) {
                        left = middle + 1;
                        continue;
                    }
                    right = middle - 1;
                }
                if (ok) {
                    data->exponent_list[i].push_back(number);
                }
            }
        }
    }
    pthread_barrier_wait(data->barrier);
    return NULL;
}

void prioritize_files_on_threads(map_data mapper_arguments[], vector<long long> numbers[], string files_names[], int mapper_threads_number, int nr_files, int exponent, pthread_barrier_t &barrier) {
    multiset<pair<int, int>> priority;
    for (int i = 0; i < mapper_threads_number; ++i) {
        priority.insert({0, i});
        mapper_arguments[i].id = i;
        mapper_arguments[i].exponent = exponent;
        mapper_arguments[i].barrier = &barrier;
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

void *formate_lists(void *arg) {
    reducer_data *data = (reducer_data *)arg;
    pthread_barrier_wait(data->barrier);
    for (int i = 0; i < data->mappers_number; ++i) {
        data->lists.push_back(data->mapper_arguments[i].exponent_list[data->exponent_to_check]);
    }
    set<long long> uniques_values;
    for (auto list : data->lists) {
        for (auto number : list) {
            uniques_values.insert(number);
        }
    }
    data->unique_values = uniques_values.size();
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
    string files_names[nr_files];
    for (int i = 0; i < nr_files; ++i) {
        in >> files_names[i];
    }

    vector<long long> numbers[nr_files];
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
    reducer_data reducer_arguments[reducer_threads_number];

    pthread_barrier_t barrier;
        
    pthread_barrier_init(&barrier, NULL, mapper_threads_number + reducer_threads_number);

    prioritize_files_on_threads(mapper_arguments, numbers, files_names, mapper_threads_number, nr_files, reducer_threads_number + 1, barrier);

    int error = 0;
    for (int i = 0; i < mapper_threads_number; ++i) {
        if (i < mapper_threads_number) {
            error = pthread_create(&mapper_threads[i], NULL, create_exponent_lists, &mapper_arguments[i]);
            if (error) {
                cout << "Error: Creation of mapper thread with number " << i << " failed!\n";
                return 1;
            }
        }
    }
    
    for (int i = 0; i < reducer_threads_number; ++i) {
        reducer_arguments[i].exponent_to_check = i + 2;
        reducer_arguments[i].unique_values = 0;
        reducer_arguments[i].mapper_arguments = mapper_arguments;
        reducer_arguments[i].barrier = &barrier;
        reducer_arguments[i].mappers_number = mapper_threads_number;
        error = pthread_create(&reducer_threads[i], NULL, formate_lists, &reducer_arguments[i]);
        if (error) {
            cout << "Error : Creation of reducer thread with number " << i << " failed!\n";
        }
    }

    void *status;
    for (int i = 0; i < mapper_threads_number; ++i) {
        error = pthread_join(mapper_threads[i], &status);
        if (error) {
            cout << "Error: Failed to join threads";
            return 1;
        }
    }

    for (int i = 0; i < reducer_threads_number; ++i) {
        error = pthread_join(reducer_threads[i], &status);
        if (error) {
            cout << "Error: Failed to join threads";
            return 1;
        }
    }

    for (int i = 0; i < reducer_threads_number; ++i) {
        string out_file_name = "out" + number_to_string(reducer_arguments[i].exponent_to_check) + ".txt";
        ofstream out(out_file_name);
        out << reducer_arguments[i].unique_values;
    }

    return 0;
}