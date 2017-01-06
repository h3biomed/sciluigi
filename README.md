![SciLuigi Logo](http://i.imgur.com/2aMT04J.png)

**Please note, this is a fork of the original [SciLuigi repo](https://github.com/pharmbio/sciluigi), created by
Samuel Lampa.  Our repo contains significant changes that have not yet been merged into the original repo, and may
never be merged.  You can find full documentation for the H3 version [here](http://pipelines-docs.h3b.hope/sciluigi).

Scientific Luigi (SciLuigi for short) is a light-weight wrapper library around [Spotify](http://spotify.com)'s [Luigi](http://github.com/spotify/luigi)
workflow system that aims to make writing scientific workflows (consisting of
numerous interdependent commandline applications) more fluent, flexible and
modular.

Luigi is a great, flexible, and very fun-to-use library. It has turned out though,
that its default way of defining dependencies by hard coding them in each task's
requires() function is not optimal for some type of workflows common e.g. in scientific
fields such as bioinformatics, where multiple inputs and outputs, complex dependencies,
and the need to quickly try different workflow connectivity (e.g. plugging in extra
filtering steps) in an explorative fashion is central to the way of working.

SciLuigi was designed to solve some of these problems, by providing the following
"features" over vanilla Luigi:

- Separation of dependency definitions from the tasks themselves,
  for improved modularity and composability.
- Inputs and outputs implemented as separate fields, a.k.a.
  "ports", to allow specifying dependencies between specific input
  and output-targets rather than just between tasks. This is again to let such
  details of the network definition reside outside the tasks.
- The fact that inputs and outputs are object fields, also allows auto-completion
  support to ease the network connection work (Works great e.g. with [jedi-vim](https://github.com/davidhalter/jedi-vim)).

Because of Luigi's great easy-to-use API, these changes have been implemented
as a very thin layer on top of luigi's own API, and no changes to the luigi
core is needed at all, so you can continue leveraging the work already being
put into maintaining and further developing luigi, by the team at Spotify and others.

Contributors
----------------
- [Samuel Lampa](https://github.com/samuell)
- [Jeff C Johnson](https://github.com/jeffcjohnson)

Acknowledgements
----------------
This work is funded by:
- [Faculty grants of the dept. of Pharmaceutical Biosciences, Uppsala University](http://www.farmbio.uu.se)
- [Bioinformatics Infrastructure for Life Sciences, BILS](https://bils.se)

Many ideas and inspiration for the API is taken from:
- [John Paul Morrison's invention and works on Flow-Based Programming](jpaulmorrison.com/fbp)
