{
  description = "Flake for zfsbackrest";

  inputs.nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

  inputs.devshell.url = "github:numtide/devshell";
  inputs.devshell.inputs.nixpkgs.follows = "nixpkgs";

  inputs.flake-compat.url = "git+https://git.lix.systems/lix-project/flake-compat";
  inputs.flake-compat.flake = false;

  outputs =
    {
      self,
      nixpkgs,
      devshell,
      ...
    }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      eachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = eachSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [ devshell.overlays.default ];
          };
        in
        {
          default = pkgs.devshell.mkShell {
            bash = {
              interactive = "";
            };

            env = [
              {
                name = "DEVSHELL_NO_MOTD";
                value = 1;
              }
            ];

            packages = with pkgs; [
              git
              go-outline
              go
              gopls
              gotools
            ];
          };
        }
      );
      packages = eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.buildGoModule (finalAttrs: {
            pname = "zfsbackrest";
            version = "0.1.0";
            src = ./.;
            env.CGO_ENABLED = 0;
            
            ldflags = [
              "-X main.version=${finalAttrs.version}"
              "-X main.date=1970-01-01"
              "-X main.commit=${self.shortRev or "unknown"}"
            ];
            
            subPackages = [
              "cmd/zfsbackrest"
            ];
            
            vendorHash = "sha256-7vtRH5ookHyEPKiI/4Dz9AZk0vfxQWbQExL8MQSZVJE=";

            meta = {
              description = "pgbackrest style encrypted backups for ZFS filesystems";
              homepage = "https://github.com/zfsbackrest/zfsbackrest";
              license = pkgs.lib.licenses.mit;
            };
          });
        }
      );
    };
}
