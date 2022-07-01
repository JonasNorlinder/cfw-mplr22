# Compressed Forwarding Tables Reconsidered (CFW)

This repository contains the code used in the paper [Compressed Forwarding Tables Reconsidered](https://doi.org/10.1145/3546918.3546928), which explores a design that guarantees a low memory bound for forwarding information used by the GC. The design is evaluated on-top of generational ZGC.

## Comparing patches

`cfw` contains the main contribution. Other branches `cfw_partial_ordinal`, `cfw_partial_vector_array`, `cfw_partial_vector_inline`, `cfw_coalesce` contains the main contribution and the optimization.

* [Compare CFW with baseline generational 
ZGC](https://github.com/JonasNorlinder/cfw-mplr22/compare/zgc_generational...cfw) ... 
or `git diff zgc_generational cfw`
* [Compare coalesce optimization with CFW](https://github.com/JonasNorlinder/cfw-mplr22/compare/cfw...cfw_coalesce) or `git diff cfw cfw_coalesce`
* [Compare partial ordinal optimization with CFW](https://github.com/JonasNorlinder/cfw-mplr22/compare/cfw...cfw_partial_ordinal) or `git diff cfw cfw_partial_ordinal`
* [Compare partial vector array optimization with CFW](https://github.com/JonasNorlinder/cfw-mplr22/compare/cfw...cfw_partial_vector_array) or `git diff cfw cfw_partial_vector_array`
* [Compare partial inline vector optimization with CFW](https://github.com/JonasNorlinder/cfw-mplr22/compare/cfw...cfw_partial_vector_inline) or `git diff cfw cfw_partial_vector_inline`

Note that baseline generational ZGC is branched from [openjdk/zgc/1bfcf4fe4ee6243bac9cf](https://github.com/openjdk/zgc/commit/1bfcf4fe4ee6243bac9cff0f828bb418bd843e6f). A small patch is applied on the baseline in order to measure in bytes instead of megabytes to avoid truncation.

## How to build and run
Building on Linux is required. We defer to the official [OpenJDK tutorial](https://openjdk.org/groups/build/doc/building.html) for a detailed description on how to build. A minimal build configuration to use is `bash configure --with-target-bits=64 --with-boot-jdk=/home/user/java/17.0.1-open`. You can build using `make CONF=release` and will find the Java binary located at `build/linux-x86_64-server-release/jdk/bin/java`.

For evaluation purposes, CFW comes with a flag (`ZMaxOffHeap`) to dynamically control when it is activated. If it is not activated the default table in ZGC will be used. As an example `-XX:ZMaxOffHeap=5.0` will activate CFW only if the forwarding table would use more than 5% of the configured Java max heap. The default value is set to 4.0%.

Moreover, we have modified the GC logger to display memory needed for forwarding information in bytes. This is enabled by supplying the flag `-Xlog:gc*`. As an example the output would look something like:

```
[0,329s][info][gc,reloc       ] GC(0) Forwarding Usage: 8451200B
```

So to evaluate CFW using h2 with a large input size from DaCapo one could write the following: `build/linux-x86_64-server-release/jdk/bin/java -XX:+UseZGC -XX:ZMaxOffHeap=4.0 -Xlog:gc* -Xms1200m -Xmx1200m -jar /home/user/dacapo-9.12-MR1-bach.jar h2 -size large -n 30 -t 4`.
