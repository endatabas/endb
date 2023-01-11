#include <stdio.h>
#include <stdint.h>

#include <sqllogictest.h>

void registerODBC3(void){
  printf("Registering ENDB engine %ld.\n", (uintptr_t) sqllogictestRegisterEngine);
}
