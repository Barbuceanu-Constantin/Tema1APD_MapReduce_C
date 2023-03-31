#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#include <vector>
using namespace std;

struct my_arg {
    int nr_mappers, nr_reducers, nr_entry_files, id_mapper, id_reducer;
    vector<vector<vector<int>>> *v;
    pthread_barrier_t *bariera;
    int *file_counter;
    char **matrix;
};

void check_perfect_power (unsigned int nr, vector<vector<vector<int>>> *v,
                          int id_mapper, int nr_reducers) {
    unsigned long long a, power;
    unsigned int exp, left, right, middle;

    //Se parcurg toate puterile posibile 
    for (exp = 2; exp < nr_reducers + 2; ++exp) {
        /*
            Pentru fiecare putere posibila se face o cautare binara.
            In acest fel se verifica daca exista un numar care ridicat
            la puterea exp sa fie egal cu nr-ul primit ca parametru.
            Daca da atunci nr e putere perfecta de exp si se pune in v.
        */
        left = 1;
        right = nr;
        while (left <= right) {
            middle = (left + right) / 2;
            a = middle;
            power = 1;
            /*
                Valoarea initiala de care se incepe este middle.
                Aceasta se ridica de maxim exp ori la putere.
                Left si right se modifica daca rezultatul final
                nu este egal cu n pt. a restrange domeniul de cautare 
            */
            while (power < exp) {
                a *= middle;
                if (a > nr) break;
                ++power;
            }
            if (a == nr) {
                (*v)[id_mapper][exp-2].push_back(nr);
                break;
            } else if (a > nr) {
                right = middle - 1;
            } else {
                left = middle + 1;
            }
        }
    }
}

void *mapper (void *arg) {
    struct my_arg* d = (struct my_arg*) arg;
    unsigned int nr_numbers, x, i;
    pthread_mutex_t mutex;
    FILE *fptr;
    int res;
    
    //Initializez mutexul pe care il voi folosi
    res = pthread_mutex_init(&mutex, NULL);
	if (res) {
        printf("Eroare alocare mutex in mapper.\n");
        exit(-1);
    }

    //Fiecare mapper ruleaza cat timp mai sunt fisiere de procesat.
    while (*(d->file_counter) < d->nr_entry_files - 1) {
        /*
            Deschid fisierul. Actualizez contorul de linie. ZONA CRITICA!!!
            Contorul trebuie incrementat de un singur thread la un anumit
            moment de timp.
        */
        res = pthread_mutex_lock(&mutex);
        if (res) {
            printf("Eroare la lock in mapper.\n");
            exit(-1);
        }

        (*(d->file_counter))++;
        fptr = fopen(d->matrix[*(d->file_counter)],"r");
        if (fptr == NULL) {
            printf("Eroare la deschiderea fisierului in mapper.\n");
            exit(-1);
        }
	    
        res = pthread_mutex_unlock(&mutex);
        if (res) {
            printf("Eroare la unlock in mapper.\n");
            exit(-1);
        }

        //Citesc numarul de numere din fisierul de input alocat mapperului.
        fscanf(fptr, "%d\n", &nr_numbers);
        
        /*  
            Citesc toate numerele din fisierul curent si verific pentru
            fiecare daca este putere perfecta. Daca da va fi introdus pe
            pozitia sa in v.
        */
        for (i = 0; i < nr_numbers; ++i) {
            fscanf(fptr, "%d\n", &x);
            if (x == 1) {
                /*
                    Daca nr-ul este 1 se introduce in toate multimile
                    de puteri ale mapperului curent.
                */
                for (i = 0; i < (*(d->v))[d->id_mapper].size(); ++i)
                    (*(d->v))[d->id_mapper][i].push_back(x); 
            } else {
                /*
                    Daca numarul nu este 1, se verifica daca este putere perfecta
                    iar daca da se va introduce pe pozitia corespunzatoare in v.
                */
                check_perfect_power(x, d->v, d->id_mapper, d->nr_reducers);
            }
        }
        fclose(fptr);
    }

    /*
        Se pune bariera la final pentru a semnaliza
        ca mapperii au terminat executia.
    */
    res = pthread_barrier_wait(d->bariera);
	if (!(res == 0 || res == PTHREAD_BARRIER_SERIAL_THREAD)) {
        printf("Eroare la asteptatul la bariera in mapper.\n");
        exit(-1);
    }

    //Se distruge mutexul.
    res = pthread_mutex_destroy(&mutex);
	if (res) {
        printf("Eroare la dezalocarea mutexului in mapper.\n");
        exit(-1);
    }

    pthread_exit(NULL);
}

void *reducer (void *arg) {
    struct my_arg* d = (struct my_arg*) arg;
    char filename[20], nr[10];
    vector<int> final_list;
    FILE *fptr;
    int res;

    //Deschid fisierul in care reducerul curent o sa scrie rezultatul.
    strcpy(filename, "out");
    snprintf(nr, sizeof(nr), "%d", d->id_reducer + 2);
    strcat(filename, nr);
    strcat(filename, ".txt");
    fptr = fopen(filename,"w");
    if (fptr == NULL) {
        printf("Eroare la deschiderea fisierului in reducer.\n");
        exit(-1);
    }

    //Se pune bariera la inceput. Pentru ca reducerii vor astepta pana termina mapperii.
    res = pthread_barrier_wait(d->bariera);
	if (!(res == 0 || res == PTHREAD_BARRIER_SERIAL_THREAD)) {
        printf("Eroare la asteptatul la bariera in reducer.\n");
        exit(-1);
    }

    /*
        Creez o lista finala de puteri perfecte unice toate cu acelasi exponent
        determinat de id-ul reducer-ului. Fiecare reducer ("coloana") stocheaza
        puteri de un anumit exponent. Deci fiecare reducer va parcurge v pe "coloana"
        data de id si va insera in lista finala numerele gasite.
    */
    for (int j = 0; j < d -> nr_mappers; ++j) {
        for (int x : (*(d->v))[j][d->id_reducer]) {
            final_list.push_back(x);
        }
    }

    //Elimin duplicatele din final_list.
    for (int i = 0; i < final_list.size() - 1; ++i) {
        for (int j = i + 1; j < final_list.size(); ++j) {
            if (final_list[j] == final_list[i]){
                final_list.erase(final_list.begin() + j);
                --j;
            }
        }
    }
    
    //Scriu rezultatul in fisier si inchid.
    fprintf(fptr, "%zu", final_list.size());
    
    fclose(fptr);

    pthread_exit(NULL);
}

int main (int argc, char *argv[]) {
    int nr_mappers, nr_reducers, nr_threads, res, nr_entry_files, file_counter;
    char input_base_file[20], **matrix;
    vector<vector<vector<int>>> v;
    pthread_barrier_t bariera;
    vector<vector<int>> aux1;
    vector<int> aux2;
    FILE *fptr;

    //Desetez bufferingul automat al afisarii. Asta m-a ajutat la debugging.
    setvbuf(stdout, NULL, _IONBF, 0);

    //Extrag parametrii din linia de comanda.
    nr_mappers = atoi(argv[1]);
    nr_reducers = atoi(argv[2]);
    nr_threads = nr_mappers + nr_reducers;
    strcpy(input_base_file, argv[3]);

    //Deschid fisierul de baza.
    struct my_arg arg[nr_threads];
    pthread_t threads[nr_threads];
    fptr = fopen(input_base_file, "r");
    if (fptr == NULL) {
        printf("Eroare la deschiderea fisierului de baza in main.\n");
        exit(-1);
    }

    //Initializez bariera dintre maperi si reduceri.
    res = pthread_barrier_init(&bariera, NULL, nr_threads);
    if(res) {
        printf("Eroare la crearea barierei in main.\n");
        exit(-1);
    }

    /*  
        Citesc nr-ul de fisiere. Aloc matricea cu numele fisierelor.
        Citesc numele fisierelor si le plasez pe linii consecutive in matrice.
    */
    fscanf(fptr, "%d\n", &nr_entry_files);
    matrix = (char **) malloc(sizeof(char *) * nr_entry_files);
    if (!matrix) {
        printf("Eroare la alocarea unei matrici de nume de fisiere.\n");
        exit(-1);
    } else {
        for (int i = 0; i < nr_entry_files; ++i) {
            matrix[i] = (char *)malloc(20);
            if (!matrix[i]) {
                printf("Eroare la alocarea unei linii in matricea de nume.\n");
                exit(-1);
            }
            fscanf(fptr, "%s\n", matrix[i]);
        }
    }

    /*  
        Initializez structura v in care voi stoca rezultatele mapperilor.
        Vor fi nr_mapperi "linii" si nr_reducers "coloane".
    */
    for (int i = 0; i < nr_mappers; ++i)
        v.push_back(aux1);
    for (int i = 0; i < nr_mappers; ++i)
        for (int j = 0; j < nr_reducers; ++j)
            v[i].push_back(aux2);

    /*
        Creez threadurile intr-un singur for. Nr_mappers mapperi dupa care reducerii.
        File counter indica fisierul curent caci accesez dinamic.
    */
    file_counter = -1;
    for (int i = 0; i < nr_threads; ++i) {
        /*
            Structura v este comuna mapperilor care nu se suprapun.
            Bariera este comuna si mapperilor si reducerilor.
            Contorul este vazut de toti mapperii pt. a putea fi actualizat in mutex.
            Matricea cu numele fisierelor este un pointer deci e vazuta de toti mapperii.
        */
        arg[i].v = &v;
        arg[i].bariera = &bariera;
        arg[i].file_counter = &file_counter;
        arg[i].nr_entry_files = nr_entry_files;
        arg[i].id_mapper = -1;
        arg[i].id_reducer = -1;
        arg[i].matrix = matrix;
        arg[i].nr_mappers = nr_mappers;
        arg[i].nr_reducers = nr_reducers;
        if (i < nr_mappers) {
            arg[i].id_mapper = i;
            pthread_create(&threads[i], NULL, mapper, &arg[i]);
        } else {
            arg[i].id_reducer = i - nr_mappers;
            pthread_create(&threads[i], NULL, reducer, &arg[i]);
        }
    }

    //Astept threadurile create.
    for (int i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], NULL);
	}

    //Distrug bariera.
    res = pthread_barrier_destroy(&bariera);
    if (res) {
        printf("Eroare distrugere bariera in main.\n");
        exit(-1);
    }

    //Dezaloc matricea de nume de fisiere.
    for (int i = 0; i < nr_entry_files; ++i) {
        free(matrix[i]);
    }
    free(matrix);

    fclose(fptr);
    return 0;
}