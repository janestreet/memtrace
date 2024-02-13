# memtrace

A streaming client for OCaml's Memprof, which generates compact traces
of a program's memory use.

To profile the memory use of a program, start by putting this line
somewhere at the program startup:

    Memtrace.trace_if_requested ~context:"my program" ();;

If the `MEMTRACE` environment variable is present, tracing begins to
the filename it specifies. (If it's absent, nothing happens)

The ~context parameter is optional, and can be set to any string that
helps to identify the trace file.

If the program daemonises, the call to `trace_if_requested` should
occur *after* the program forks, to ensure the right process is
traced.

The resulting trace files can be analysed with some simple
command-line tools in bin/, but the recommended interface is the
memtrace viewer, which lives at:

    https://github.com/janestreet/memtrace_viewer

## Installation
These instructions are for using statmemprof with OCaml 5.* based on
[ocaml#12923](https://github.com/ocaml/ocaml/pull/12923) which restores statmemprof for multicore plus
[ocaml#12963](https://github.com/ocaml/ocaml/pull/12963) which fixes some runtime parameter queries.

``` shell
# Setup a new Blank switch
opam switch create statmemprof --empty

# Install OCaml trunk with #12923 + #12963
opam pin add ocaml-variants git+https://github.com/tmcgilchrist/ocaml#caml_runtime_parameters

# Install a patched dune
opam pin -n git+https://github.com/ocaml/dune.git#main --with-version=3.13.0 -y
```

Now we can get memory traces for programs, here is a multicore fibonacci program:

``` shell
# Run tracing on single domain
MEMTRACE=fib_par.ctf _build/default/examples/fib_par.exe 1 80

# On three domains
MEMTRACE=fib_par_2.ctf _build/default/examples/fib_par.exe 3 45
```

these CTF files are viewable in `memtrace_viewer`.

Install memtrace_viewer in another switch (5.1 will work since we only need to read and write trace files)

``` shell
opam switch create 5.1.1 --no-install
opam install memtrace_viewer
memtrace-viewer ./fib_par_2.ctf
```

