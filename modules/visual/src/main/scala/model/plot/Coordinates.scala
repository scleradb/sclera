/**
* Sclera - Visualization
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.visual.model.plot

/** Coordinates */
sealed abstract class Coordinates

object Coordinates {
    lazy val grid: CoordGrid = grid(GridType.cartesian, GridAes.default)

    def grid(
        gridType: GridType,
        aes: GridAes
    ): CoordGrid = CoordGrid(gridType, aes)

    lazy val map: CoordMap =
        map(MapProject.orthographic, MapOrient(0.0, 0.0, 0.0))

    def map(
        proj: MapProject,
        orient: MapOrient
    ): CoordMap = CoordMap(proj, orient)
}

case class CoordGrid(
    gridType: GridType,
    aes: GridAes
) extends Coordinates

case class CoordMap(
    proj: MapProject,
    orient: MapOrient
) extends Coordinates

/** Grid type */
sealed abstract class GridType

object GridType {
    def cartesian: GridType = Cartesian
    def polar: GridType = Polar

    def apply(s: String): GridType = s.toUpperCase match {
        case "CARTESIAN" => cartesian
        case "POLAR" => polar
        case _ =>
            throw new IllegalArgumentException(
                "Invalid grid specification: " + s
            )
    }
}

case object Cartesian extends GridType
case object Polar extends GridType

/** Map projection */
sealed abstract class MapProject

object MapProject {
    def azimuthalEqualArea: MapProject = AzimuthalEqualArea

    def azimuthalEquiDistant: MapProject = AzimuthalEquiDistant

    def conicConformal(
        parallelA: Double,
        parallelB: Double
    ): ConicConformal = ConicConformal(parallelA, parallelB)

    def conicEquiDistant(
        parallelA: Double,
        parallelB: Double
    ): ConicEquiDistant = ConicEquiDistant(parallelA, parallelB)

    def equiRectangular: MapProject = EquiRectangular

    def mercator: MapProject = Mercator

    def orthographic: MapProject = Orthographic

    def stereographic: MapProject = Stereographic

    def transverseMercator: MapProject = TransverseMercator

    def apply(name: String, paramsOpt: Option[(Double, Double)]): MapProject =
        (name.toUpperCase, paramsOpt) match {
            case ("AZIMUTHALEQUALAREA", None) => azimuthalEqualArea
            case ("AZIMUTHALEQUIDISTANT", None) => azimuthalEquiDistant
            case ("CONICCONFORMAL", Some((a, b))) => conicConformal(a, b)
            case ("CONICEQUIDISTANT", Some((a, b))) => conicEquiDistant(a, b)
            case ("EQUIRECTANGULAR", None) => equiRectangular
            case ("MERCATOR", None) => mercator
            case ("ORTHOGRAPHIC", None) => orthographic
            case ("STEREOGRAPHIC", None) => stereographic
            case ("TRANSVERSEMERCATOR", None) => transverseMercator
            case _ => throw new IllegalArgumentException(
                "Cannot interpret as map projection: " + name +
                paramsOpt.getOrElse("")
            )
        }
}

case object AzimuthalEqualArea extends MapProject
case object AzimuthalEquiDistant extends MapProject

case class ConicConformal(
    parallelA: Double,
    parallelB: Double
) extends MapProject

case class ConicEquiDistant(
    parallelA: Double,
    parallelB: Double
) extends MapProject

case object EquiRectangular extends MapProject
case object Mercator extends MapProject
case object Orthographic extends MapProject
case object Stereographic extends MapProject
case object TransverseMercator extends MapProject

case class MapOrient(a: Double, b: Double, c: Double)
