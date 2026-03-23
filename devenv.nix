{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  packages = [
    pkgs.git
    pkgs.nixfmt-rfc-style
  ]
  ++ lib.optionals pkgs.stdenv.isLinux [
    pkgs.inotify-tools
  ];

  languages.elixir = {
    enable = true;
    package = pkgs.elixir_1_19;
  };

  services.clickhouse = {
        enable = true;
        config = ''
          listen_host: "127.0.0.1"
        '';
      };

  env.ERL_AFLAGS = "-kernel shell_history enabled";

  enterShell = ''
    mix local.hex --force --if-missing
    mix local.rebar --force --if-missing
  '';
}
