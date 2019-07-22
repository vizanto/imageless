# This defines a function taking `pkgs` as parameter, and uses
# `nixpkgs` by default if no argument is passed to it.
{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
  buildInputs = [
    coreutils
    openssl
    git
    nodejs-10_x
    openjdk
    yarn
    # nodePackages.pnpm
    # haxe
  ];
}
