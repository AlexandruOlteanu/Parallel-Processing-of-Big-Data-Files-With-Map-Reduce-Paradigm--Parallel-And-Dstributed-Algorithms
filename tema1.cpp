/*
 *  <Copyright Alexandru Olteanu, grupa 332CA, alexandruolteanu2001@gmail.com>
 */

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

/**
 * Realizeaza ridicarea numarului a la puterea b in timp logaritmic, 
 * returnand -1 daca a^b < c, 0 daca a^b == c sau 1 daca a^b > c
 * */
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

/**
 * Formeaza listele pentru fiecare exponent in parte, folosind cautare binara 
 * si ridicare la putere in timp logaritmic
 * */
void *create_exponent_lists(void *arg) {

    map_data *data = (map_data *) arg;
    
    int exponent = data->exponent;
    for (auto file : data->files) {
        for (auto number : file.numbers) {
            // Daca numarul este 1, acesta ajunge in toate listele, conditia este separata pentru a 
            // optimiza cazurile cu exponentul foarte mare
            if (number == 1) {
                for (long long i = 2; i <= exponent; ++i) {
                    data->exponent_list[i].push_back(number);
                }
                continue;
            }
            // Se parcurge fiecare putere posibila
            for (long long i = 2; i <= exponent; ++i) {
                // Incheiem bucla curenta in cazul in care pana si cel mai mic numar are 
                // puterea mai mare decat numarul dorit, astfel cautarea binara fiind 
                // irelevanta
                if (check(2, i, number) > 0) {
                    break;
                }
                // Puterea fiind cel putin >= 2, are sens sa facem cautare binara doar pana 
                // la radicalul acestuia. Vom cauta pe x cu cautare binara astel incat x^i = number
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
                // Daca numarul face parte din lista exponentului curent, il adaugam
                // in argument
                if (ok) {
                    data->exponent_list[i].push_back(number);
                }
            }
        }
    }
    // Thread-ul mapper curent asteapta la bariera dupa ce a terminat procesul
    pthread_barrier_wait(data->barrier);
    return NULL;
}

/**
 * Functie pentru prioritizarea fisierelor pe thread-urile mapper. Prioritizarea a fost 
 * facuta in functie de numarul de numere de verificat, nu cel de fisiere pentru o 
 * optimizarea cat mai buna
 * */
void prioritize_files_on_threads(map_data mapper_arguments[], vector<long long> numbers[], string files_names[], int mapper_threads_number, int nr_files, int exponent, pthread_barrier_t &barrier) {
    // Construim un multiset care ne mentine sub forma de pereche,
    // mapperele sortate crescator dupa numarul de numere pe care le au 
    // dupa care iteram prin fisiere si atribuim mereu celui mai liber 
    // urmatorul load de munca
    multiset<pair<int, int>> priority;
    for (int i = 0; i < mapper_threads_number; ++i) {
        priority.insert({0, i});
        mapper_arguments[i].id = i;
        mapper_arguments[i].exponent = exponent;
        mapper_arguments[i].barrier = &barrier;
        mapper_arguments[i].exponent_list.resize(exponent + 3);
    }
    // Pentru a face acest proces, eliminam mereu primul element si 
    // il adaugam e cel updatat dupa noul task primit
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

/**
 * Functie ce parseaza listele de exponenti si se foloseste de 
 * un set pentru a include doar valori unice. In final, dimensiunea
 * setului reprezinta numarul final de elemente unice ce trebuie
 * afisat la output
 * */
void *formate_lists(void *arg) {
    reducer_data *data = (reducer_data *)arg;
    // Reducerele asteapta la bariera la inceput pentru a progresa
    // doar dupa ce si mapperele au terminat, astfel rezolvand pe listele
    // de exponenti finale
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

    // Programare defensiva pentru numarul de argumente primite de utilizator
    if (argc != NUMBER_OF_ARGUMENTS) {
        cout << "Error: Wrong number of Arguments\n";
        return 1;
    }

    // Se realizeaza citirea numerelor din fisiere si stocarea datelor intr-o structura
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

    // Aflam numarul de thread-uri mapper si reducer si initializam 
    // bariera
    int mapper_threads_number = string_to_number(argv[1]);
    pthread_t mapper_threads[mapper_threads_number];
    map_data mapper_arguments[mapper_threads_number];

    int reducer_threads_number = string_to_number(argv[2]);
    pthread_t reducer_threads[reducer_threads_number];
    reducer_data reducer_arguments[reducer_threads_number];

    pthread_barrier_t barrier;
    
    // Bariera are la initializare reducer_nr + mapper_nr threaduri pentru
    // a porni doar dupa ce toate au ajuns la aceasta 
    pthread_barrier_init(&barrier, NULL, mapper_threads_number + reducer_threads_number);

    // Facem prioritizarea statica a fisierelor pentru threadurile mapper
    prioritize_files_on_threads(mapper_arguments, numbers, files_names, mapper_threads_number, nr_files, reducer_threads_number + 1, barrier);

    // Creem thredurile de mapper cu argumentele corespunzatoare
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

    // Creem thredurile de reducer, ca argumente avand si un pointer la listele
    // de mapper, astfel ele modificandu-se in timp ce reducerele asteapta initial la
    // bariera
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

    // Realizam join-ul tuturor threadurilor
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

    // Parcurgem resultatele formate de reducer si afisam in fisierele corespunzatoare rezultatul final
    for (int i = 0; i < reducer_threads_number; ++i) {
        string out_file_name = "out" + number_to_string(reducer_arguments[i].exponent_to_check) + ".txt";
        ofstream out(out_file_name);
        out << reducer_arguments[i].unique_values;
    }

    return 0;
}