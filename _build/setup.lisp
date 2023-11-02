;; Dependencies are submodules below _build, which isn't searched by
;; default in ASDF. To setup the source registry for development, run:
;;
;; (load "_build/setup.lisp")

;; The system is then loaded like normal via asdf:load-system.
;; This also allows the project to be cloned anywhere.

;; The submodule dependencies are based on the following sources.

;; https://github.com/quicklisp/quicklisp-projects
;; https://git.savannah.gnu.org/cgit/guix.git/tree/gnu/packages/lisp-xyz.scm
;; https://github.com/ocicl

(require :asdf)

(unless (equal "1" (uiop:getenv "ENDB_COMPILE_VERBOSE"))
  (setf uiop:*uninteresting-conditions* (cons 'warning uiop:*usual-uninteresting-conditions*))
  (setf *compile-verbose* nil))

(let ((inherit-config (if (equal "1" (uiop:getenv "ENDB_ASDF_INHERIT_CONFIGURATION"))
                          :inherit-configuration
                          :ignore-inherited-configuration))
      (asdf/source-registry:*recurse-beyond-asds* nil))
  (asdf:initialize-source-registry
   `(:source-registry
     ,inherit-config
     (:directory (:here ".."))
     (:tree (:here ".")))))
