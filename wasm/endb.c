#include <stdio.h>
#include <stdlib.h>
#include <ecl/ecl.h>

static char *c_str_eval_buffer = NULL;
static cl_fixnum c_str_eval_buffer_size = 0;

char *common_lisp_eval(char *src) {
  cl_object form = ecl_read_from_cstring(src);
  cl_object result = si_safe_eval(2, form, ECL_NIL);
  cl_object str =  cl_prin1_to_string(result);
  cl_fixnum res = ecl_encode_to_cstring(c_str_eval_buffer, c_str_eval_buffer_size, str, ecl_make_keyword("UTF-8"));
  if (res > c_str_eval_buffer_size) {
    free(c_str_eval_buffer);
    c_str_eval_buffer = malloc(res);
    if (c_str_eval_buffer == NULL) {
      abort();
    }
    c_str_eval_buffer_size = res;
    res = ecl_encode_to_cstring(c_str_eval_buffer, c_str_eval_buffer_size, str, ecl_make_keyword("UTF-8"));
  }
  if (res == -1) {
    return NULL;
  }
  return c_str_eval_buffer;
}

int main(int argc, char **argv) {
  cl_boot(argc, argv);

  extern void init_lib_endb_lisp(cl_object);
  ecl_init_module(NULL, init_lib_endb_lisp);

  // cl_shutdown();
  return 0;
}
