#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#define MAXCHAR 500

#define LEN_CODE_AIRPORT 3
#define STR_CODE_AIRPORT (LEN_CODE_AIRPORT+1) // Incluimos el caracter de final de palabra '\0'
#define NUM_AIRPORTS 303

#define COL_ORIGIN_AIRPORT 17
#define COL_DESTINATION_AIRPORT 18
#define F 2
#define N 10

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

struct args
  {
    FILE* fp;
    int **num_flights;
    char **airports;
  };

/**
 * Reserva espacio para una matriz de tamaño nrow x ncol,
 * donde cada elemento de la matriz tiene size bytes
 */

void **malloc_matrix(int nrow, int ncol, size_t size)
{
  int i;

  void **ptr;
  ptr = calloc(nrow, sizeof(void*));
  for(i = 0; i < nrow; i++){
    ptr[i] = calloc(ncol,size);
  }

  return ptr;
}

/**
 * Libera una matriz de tamaño con nrow filas. Utilizar
 * la funcion malloc_matrix para reservar la memoria 
 * asociada.
 */

void free_matrix(void **matrix, int nrow)
{
  int i;

  for(i = 0; i < nrow; i++)
    free(matrix[i]);
}

/**
 * Leer el fichero fname que contiene los codigos
 * IATA (3 caracteres) de los aeropuertos a analizar.
 * En total hay NUM_AIRPORTS a leer, un valor prefijado
 * (para simplificar el código). Los codigos de los
 * aeropuertos se alacenan en la matriz airports, una
 * matriz cuya memoria ha sido previamente reservada.
 */

void read_airports(char **airports, char *fname) 
{
  int i;
  char line[MAXCHAR];

  FILE *fp;

  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';

  fp = fopen(fname, "r");
  if (!fp) {
    printf("ERROR: could not open file '%s'\n", fname);
    exit(1);
  }

  i = 0;
  while (i < NUM_AIRPORTS)
  {
    fgets(line, 100, fp);
    line[3] = eow; 

    /* Copiamos los datos al vector */
    strcpy(airports[i], line);

    i++;
  }

  fclose(fp);
}

/**
 * Dada la matriz de con los codigos de los aeropuertos,
 * así como un código de aeropuerto, esta función retorna
 * la fila asociada al aeropuerto.
 */

int get_index_airport(char *code, char **airports)
{
  int i;

  for(i = 0; i < NUM_AIRPORTS; i++) 
    if (strcmp(code, airports[i]) == 0)
      return i;

  return -1;
}


/**
 * Dada la matriz num_flights, se imprimen por pantalla el
 * numero de destinos diferentes que tiene cada aeropuerto.
 */

void print_num_flights_summary(int **num_flights, char **airports)
{
  int i, j, num;
  for(i = 0; i < NUM_AIRPORTS; i++) 
  {
    num = 0;

    for(j = 0; j < NUM_AIRPORTS; j++)
    {
      if (num_flights[i][j] > 0)
        num++;
    }

    printf("Origin: %s -- Number of different destinations: %d\n", airports[i], num);
  }
}

/**
 * Esta funcion se utiliza para extraer informacion del fichero CSV que
 * contiene informacion sobre los vuelos. En particular, dada una linea
 * leida de fichero, la funcion extra el origen y destino de los vuelos.
 */

int extract_fields_airport(char *origin, char *destination, char *line) 
{
  /*Recorre la linea por caracteres*/
  char caracter;
  /* i sirve para recorrer la linea
   * iterator es para copiar el substring de la linea a char
   * coma_count es el contador de comas
   */
  int i, iterator, coma_count;
  /* start indica donde empieza el substring a copiar
   * end indica donde termina el substring a copiar
   * len indica la longitud del substring
   */
  int start, end, len;
  /* invalid nos permite saber si todos los campos son correctos
   * 1 hay error, 0 no hay error 
   */
  int invalid = 0;
  /* found se utiliza para saber si hemos encontrado los dos campos:
   * origen y destino
   */
  int found = 0;
  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';
  /*
   * contenedor del substring a copiar
   */
  char word[STR_CODE_AIRPORT];
  /*
   * Inicializamos los valores de las variables
   */
  start = 0;
  end = -1;
  i = 0;
  coma_count = 0;
  /*
   * Empezamos a contar comas
   */
  do {
    caracter = line[i++];
    if (caracter == ',') {
      coma_count ++;
      /*
       * Cogemos el valor de end
       */
      end = i;
      /*
       * Si es uno de los campos que queremos procedemos a copiar el substring
       */
      if (coma_count ==  COL_ORIGIN_AIRPORT || coma_count == COL_DESTINATION_AIRPORT) {
        /*
         * Calculamos la longitud, si es mayor que 1 es que tenemos 
         * algo que copiar
         */
        len = end - start;

        if (len > 1) {

          if (len > STR_CODE_AIRPORT) {
            printf("ERROR len code airport\n");
            exit(1);
          }

          /*
           * Copiamos el substring
           */
          for(iterator = start; iterator < end-1; iterator ++){
            word[iterator-start] = line[iterator];
          }
          /*
           * Introducimos el caracter de fin de palabra
           */
          word[iterator-start] = eow;
          /*
           * Comprobamos que el campo no sea NA (Not Available) 
           */
          if (strcmp("NA", word) == 0)
            invalid = 1;
          else {
            switch (coma_count) {
              case COL_ORIGIN_AIRPORT:
                strcpy(origin, word);
                found++;
                break;
              case COL_DESTINATION_AIRPORT:
                strcpy(destination, word);
                found++;
                break;
              default:
                printf("ERROR in coma_count\n");
                exit(1);
            }
          }

        } else {
          /*
           * Si el campo esta vacio invalidamos la linea entera 
           */

          invalid = 1;
        }
      }
      start = end;
    }
  } while (caracter && invalid==0);

  if (found != 2)
    invalid = 1;

  return invalid;
}

/**
 * Dado un fichero CSV que contiene informacion entre multiples aeropuertos,
 * esta funcion lee cada linea del fichero y actualiza la matriz num_flights
 * para saber cuantos vuelos hay entre cada cuidad origen y destino.
 */ 

void* read_airports_data(void *arg) 
{
  char line[MAXCHAR];
  char origin[STR_CODE_AIRPORT], destination[STR_CODE_AIRPORT];
  int invalid, index_origin, index_destination;


  struct args *arg_pt = (struct args *) arg;


  //N definido en la cabecera

  /* Leemos N lineas siempre y cuando no nos salgamos del fichero.
     En caso de salirnos del fichero, paramos el bucle*/

  while (!feof(arg_pt->fp)){ //Repetir hasta que el fichero se acabe
    pthread_mutex_lock(&lock);
    for (int i = 0; i < N; i++){
      //Leer datos del fichero
      if (fgets(line, MAXCHAR, arg_pt->fp) == NULL){
        break;
      }

      else{
        //Escribir datos en num_flights
        invalid = extract_fields_airport(origin, destination, line);

        if (!invalid) {
          index_origin = get_index_airport(origin, arg_pt->airports);
          index_destination = get_index_airport(destination, arg_pt->airports);
          
          if ((index_origin >= 0) && (index_destination >= 0))
            arg_pt->num_flights[index_origin][index_destination]++;
        }
      }
    }
    pthread_mutex_unlock(&lock);
  }
  return ((void *)0);
}

/**
 * Esta es la funcion principal que realiza los siguientes procedimientos
 * a) Lee los codigos IATA del fichero de aeropuertos
 * b) Lee la informacion de los vuelos entre diferentes aeropuertos y
 *    actualiza la matriz num_flights correspondiente.
 * c) Se imprime para cada aeropuerto origen cuantos destinos diferentes
 *    hay.
 * d) Se imprime por pantalla lo que ha tardado el programa para realizar
 *    todas estas tareas.
 */


int main(int argc, char **argv)
{
  char **airports;
  int ** num_flights, err, i;
  char line[MAXCHAR];
  FILE * fp;

  pthread_t ntid[F];
  pthread_mutex_init(&lock, NULL);


  if (argc != 3) {
    printf("%s <airport.csv> <flights.csv>\n", argv[0]);
    exit(1);
  }

  struct timeval tv1, tv2;
  // Tiempo cronologico 
  gettimeofday(&tv1, NULL);
  
  // Reserva espacio para las matrices
  airports    = (char **) malloc_matrix(NUM_AIRPORTS, STR_CODE_AIRPORT, sizeof(char));
  num_flights = (int **) malloc_matrix(NUM_AIRPORTS, NUM_AIRPORTS, sizeof(int));

  // Lee los codigos de los aeropuertos 
  read_airports(airports, argv[1]);
 
 


  fp = fopen(argv[2], "r");
  struct args args_pt;
  args_pt.num_flights = num_flights;
  args_pt.fp = fp;
  args_pt.airports = airports;
  /* Leemos la cabecera del fichero */
  fgets(line, MAXCHAR, fp);



  // Lee los datos de los vuelos
  for (i = 0; i < F; i++){
    err = pthread_create(ntid+i, NULL, read_airports_data, (void*) &args_pt);
    if (err != 0) {
      printf("no puc crear el fil.\n");
      exit(1);
    }
  }

  for (i = 0; i < F; i++){
    if (pthread_join(ntid[i], NULL) != 0){
      exit(2);
    }
    
  }

  // Imprime un resumen de la tabla
  print_num_flights_summary(num_flights, airports);
  //Cerrar fichero
  fclose(fp);
  // Libera espacio
  free_matrix((void **) airports, NUM_AIRPORTS);
  free(airports);
  free_matrix((void **) num_flights, NUM_AIRPORTS);
  free(num_flights);
  pthread_mutex_destroy(&lock);
  // Tiempo cronologico 
  gettimeofday(&tv2, NULL);

  // Tiempo para la creacion del arbol 
  printf("Tiempo para procesar el fichero: %f segundos\n",
      (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
      (double) (tv2.tv_sec - tv1.tv_sec));

  return 0;
}
