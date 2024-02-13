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
These instructions are for using statmemprof with OCaml 5.3.0+trunk

``` shell
# Setup a new Blank switch
opam switch create 5.3.0 --no-install
eval $(opam env --switch=5.3.0 --set-switch)

# Install dune
opam install dune
```

Now we can get memory traces for programs, here is a multicore fibonacci program:

``` shell
$ opam instal domainslib

$ dune build examples

# Run tracing on single domain
$ MEMTRACE=fib_par.ctf _build/default/examples/fib_par.exe 1 45

# On three domains
$ MEMTRACE=fib_par_2.ctf _build/default/examples/fib_par.exe 3 45
```

these CTF files are viewable in `memtrace_viewer`.

Install memtrace_viewer in another switch (5.1 will work since we only need to read and write trace files)

``` shell
opam switch create 5.1.1 --no-install
opam install memtrace_viewer
memtrace-viewer ./fib_par_2.ctf
```

