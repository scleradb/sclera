# Sclera - Visualization
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-visual_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-visual_2.13)
[![scaladoc](https://javadoc.io/badge2/com.scleradb/sclera-visual_2.13/scaladoc.svg)](https://javadoc.io/doc/com.scleradb/sclera-visual_2.13)

Visualization extension for Sclera.

Implements a very expressive graphics language to plot the results of a query as regular, multilayered and faceted graphs in just a few lines of code. The graph specification language is inspired by the "Grammar of Graphics" (implemented in `R` as [ggplot2](http://ggplot2.org)), and is rendered using [D3](http://d3js.org) in [SVG](https://en.wikipedia.org/wiki/Scalable_Vector_Graphics).

Unlike `ggplot2`, the resulting plots are interactive, and can display streaming data in a continuous manner. Moreover, the specification language is well-integrated with [ScleraSQL](https://www.scleradb.com/docs/sclerasql/sqlintro/).

For details and examples on using these constructs, please refer to the [ScleraSQL Visualization Reference](https://www.scleradb.com/docs/sclerasql/visualization/) document.
