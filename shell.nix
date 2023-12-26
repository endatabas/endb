{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  nativeBuildInputs = with pkgs.buildPackages; [
    # lisp
    sbcl

    # rust
    cargo
    rustc
    clippy

    # sqllogictest dependencies
    clang

    # endb_server crate's build script requires git
    git
  ];

  buildInputs = with pkgs; [
  ] ++ lib.optional stdenv.isDarwin libiconv;

  packages = with pkgs; [
    # rust editor tools
    rustfmt
    rust-analyzer

    # python for examples and console
    python311
  ];
}
