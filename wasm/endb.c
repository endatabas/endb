#include <stdio.h>
#include <ecl/ecl.h>

char* endb_eval(char* src) {
  cl_object form = ecl_read_from_cstring(src);
  cl_object result = si_safe_eval(2, form, ECL_NIL);
  cl_object str =  cl_prin1_to_string(result);
  static char c_str[65536];
  int res = ecl_encode_to_cstring(c_str, 65536, str, ecl_make_keyword("UTF-8"));
  if (res == -1) {
    return "NIL";
  } else {
    return c_str;
  }
}

int main(int argc, char **argv) {
  cl_boot(argc, argv);

  si_safe_eval(2, ecl_read_from_cstring("(require :asdf)"), ECL_NIL);

  extern void init_lib_endb_lisp(cl_object);
  ecl_init_module(NULL, init_lib_endb_lisp);

  //  cl_shutdown();
  return 0;
}
