{
  outputs = inputs: inputs.parts.lib.mkFlake { inherit inputs; } {
    systems = import inputs.systems;

    perSystem = { lib, pkgs, system, self', ... }: {
      _module.args = lib.fix (self: {
        lib = with inputs; builtins // nixpkgs.lib // parts.lib;
        pkgs = import inputs.nixpkgs { inherit system; };
      });

      devShells.default = pkgs.mkShell {
        inputsFrom = lib.attrValues self'.packages;
        packages = with pkgs; [
          deno
          nixpkgs-fmt

          xcaddy

          go
          go-tools
          gopls
        ];
      };

      formatter = pkgs.writeShellScriptBin "formatter" ''
        set -eoux pipefail
        shopt -s globstar

        pushd "$(${lib.getExe pkgs.git} rev-parse --show-toplevel)" > /dev/null

        ${lib.getExe pkgs.deno} fmt **/*.md
        ${lib.getExe pkgs.nixpkgs-fmt} .

        ${lib.getExe pkgs.go} fix ./...
        ${lib.getExe pkgs.go} fmt ./...
        ${lib.getExe pkgs.go} mod tidy
        ${lib.getExe pkgs.go} test -race ./...
        ${lib.getExe pkgs.go} vet ./...
        ${lib.getExe' pkgs.go-tools "staticcheck"} ./...

        popd
      '';
    };
  };

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  inputs.systems.url = "github:nix-systems/default";
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.parts.inputs.nixpkgs-lib.follows = "nixpkgs";
  inputs.utils.url = "github:numtide/flake-utils";
  inputs.utils.inputs.systems.follows = "systems";
}
