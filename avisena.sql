-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 09-10-2025 a las 18:25:29
-- Versión del servidor: 10.4.32-MariaDB
-- Versión de PHP: 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de datos: `avisena`
--

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `aislamiento`
--

CREATE TABLE `aislamiento` (
  `id_aislamiento` int(10) UNSIGNED NOT NULL,
  `id_incidente_gallina` int(10) UNSIGNED NOT NULL,
  `fecha_hora` datetime NOT NULL,
  `id_galpon` tinyint(3) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `categoria_inventario`
--

CREATE TABLE `categoria_inventario` (
  `id_categoria` tinyint(3) UNSIGNED NOT NULL,
  `nombre` varchar(30) NOT NULL,
  `descripcion` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `detalle_huevos`
--

CREATE TABLE `detalle_huevos` (
  `id_detalle` int(10) UNSIGNED NOT NULL,
  `id_producto` smallint(5) UNSIGNED NOT NULL,
  `cantidad` smallint(6) NOT NULL,
  `id_venta` int(10) UNSIGNED NOT NULL,
  `valor_descuento` decimal(10,0) NOT NULL,
  `precio_venta` decimal(10,0) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `detalle_salvamento`
--

CREATE TABLE `detalle_salvamento` (
  `id_detalle` int(10) UNSIGNED NOT NULL,
  `id_producto` int(10) UNSIGNED NOT NULL,
  `cantidad` smallint(6) NOT NULL,
  `id_venta` int(10) UNSIGNED NOT NULL,
  `valor_descuento` decimal(10,0) NOT NULL,
  `precio_venta` decimal(10,0) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `fincas`
--

CREATE TABLE `fincas` (
  `id_finca` int(10) UNSIGNED NOT NULL,
  `nombre` varchar(30) NOT NULL,
  `longitud` float NOT NULL,
  `latitud` float NOT NULL,
  `estado` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `galpones`
--

CREATE TABLE `galpones` (
  `id_galpon` tinyint(3) UNSIGNED NOT NULL,
  `id_finca` int(10) UNSIGNED NOT NULL,
  `nombre` varchar(30) NOT NULL,
  `capacidad` smallint(6) NOT NULL,
  `cant_actual` smallint(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `incidentes_gallina`
--

CREATE TABLE `incidentes_gallina` (
  `id_inc_gallina` int(10) UNSIGNED NOT NULL,
  `galpon_origen` tinyint(3) UNSIGNED NOT NULL,
  `tipo_incidente` enum('Enfermedad','Herida','Muerte','Fuga','Ataque Depredador','Produccion','Alimentacion','Plaga','Estres termico','Otro') NOT NULL,
  `cantidad` smallint(6) NOT NULL,
  `descripcion` varchar(255) NOT NULL,
  `fecha_hora` datetime NOT NULL,
  `esta_resuelto` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `incidentes_generales`
--

CREATE TABLE `incidentes_generales` (
  `id_incidente` int(10) UNSIGNED NOT NULL,
  `descripcion` varchar(255) NOT NULL,
  `fecha_hora` datetime NOT NULL,
  `id_finca` int(10) UNSIGNED NOT NULL,
  `esta_resuelta` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ingreso_gallinas`
--

CREATE TABLE `ingreso_gallinas` (
  `id_ingreso` smallint(5) UNSIGNED NOT NULL,
  `id_galpon` tinyint(3) UNSIGNED NOT NULL,
  `fecha` date NOT NULL,
  `id_tipo_gallina` tinyint(3) UNSIGNED NOT NULL,
  `cantidad_gallinas` smallint(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `inventario_finca`
--

CREATE TABLE `inventario_finca` (
  `id_inventario` int(10) UNSIGNED NOT NULL,
  `nombre` varchar(30) NOT NULL,
  `cantidad` smallint(6) NOT NULL,
  `unidad_medida` enum('Lb','Kg') NOT NULL,
  `descripcion` varchar(100) NOT NULL,
  `id_categoria` tinyint(3) UNSIGNED NOT NULL,
  `id_finca` int(10) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `metodo_pago`
--

CREATE TABLE `metodo_pago` (
  `id_tipo` tinyint(3) UNSIGNED NOT NULL,
  `nombre` varchar(30) NOT NULL,
  `descripcion` varchar(100) NOT NULL,
  `estado` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `modulos`
--

CREATE TABLE `modulos` (
  `id_modulo` tinyint(3) UNSIGNED NOT NULL,
  `nombre_modulo` varchar(30) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `permisos`
--

CREATE TABLE `permisos` (
  `id_modulo` tinyint(3) UNSIGNED NOT NULL,
  `id_rol` tinyint(3) UNSIGNED NOT NULL,
  `insertar` tinyint(1) NOT NULL,
  `actualizar` tinyint(1) NOT NULL,
  `seleccionar` tinyint(1) NOT NULL,
  `borrar` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `produccion_huevos`
--

CREATE TABLE `produccion_huevos` (
  `id_produccion` int(10) UNSIGNED NOT NULL,
  `id_galpon` tinyint(3) UNSIGNED NOT NULL,
  `cantidad` int(11) NOT NULL,
  `fecha` date NOT NULL,
  `id_tipo_huevo` tinyint(3) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `registro_sensores`
--

CREATE TABLE `registro_sensores` (
  `id_registro` int(10) UNSIGNED NOT NULL,
  `id_sensor` tinyint(3) UNSIGNED NOT NULL,
  `dato_sensor` float NOT NULL,
  `fecha_hora` datetime NOT NULL,
  `u_medida` enum('?C','lm','%') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `roles`
--

CREATE TABLE `roles` (
  `id_rol` tinyint(3) UNSIGNED NOT NULL,
  `nombre_rol` varchar(30) NOT NULL,
  `descripcion` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `salvamento`
--

CREATE TABLE `salvamento` (
  `id_salvamento` int(10) UNSIGNED NOT NULL,
  `id_galpon` tinyint(3) UNSIGNED DEFAULT NULL,
  `fecha` date NOT NULL,
  `id_tipo_gallina` tinyint(3) UNSIGNED NOT NULL,
  `cantidad_gallinas` smallint(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `sensores`
--

CREATE TABLE `sensores` (
  `id_sensor` tinyint(3) UNSIGNED NOT NULL,
  `nombre` varchar(255) NOT NULL,
  `id_tipo_sensor` tinyint(3) UNSIGNED NOT NULL,
  `id_galpon` tinyint(3) UNSIGNED NOT NULL,
  `descripcion` varchar(140) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `stock`
--

CREATE TABLE `stock` (
  `id_producto` smallint(5) UNSIGNED NOT NULL,
  `unidad_medida` enum('unidad','panal','docena','medio_panal') NOT NULL,
  `id_produccion` int(10) UNSIGNED NOT NULL,
  `cantidad_disponible` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tareas`
--

CREATE TABLE `tareas` (
  `id_tarea` smallint(5) UNSIGNED NOT NULL,
  `id_usuario` int(10) UNSIGNED NOT NULL,
  `descripcion` varchar(180) NOT NULL,
  `fecha_hora_init` datetime NOT NULL,
  `estado` enum('Asignada','Pendiente','En proceso','Completada','Cancelada') NOT NULL,
  `fecha_hora_fin` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tipo_gallinas`
--

CREATE TABLE `tipo_gallinas` (
  `id_tipo_gallinas` tinyint(3) UNSIGNED NOT NULL,
  `raza` varchar(30) NOT NULL,
  `descripcion` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tipo_sensores`
--

CREATE TABLE `tipo_sensores` (
  `id_tipo` tinyint(3) UNSIGNED NOT NULL,
  `nombre` varchar(70) NOT NULL,
  `descripcion` varchar(255) NOT NULL,
  `modelo` varchar(70) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `usuarios`
--

CREATE TABLE `usuarios` (
  `id_usuario` int(10) UNSIGNED NOT NULL,
  `nombre` varchar(70) NOT NULL,
  `id_rol` tinyint(3) UNSIGNED NOT NULL,
  `email` varchar(100) NOT NULL,
  `telefono` char(15) NOT NULL,
  `documento` varchar(20) NOT NULL,
  `pass_hash` varchar(140) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ventas`
--

CREATE TABLE `ventas` (
  `id_venta` int(10) UNSIGNED NOT NULL,
  `fecha_hora` datetime NOT NULL,
  `id_usuario` int(10) UNSIGNED NOT NULL,
  `tipo_pago` tinyint(3) UNSIGNED NOT NULL,
  `total` decimal(10,0) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `aislamiento`
--
ALTER TABLE `aislamiento`
  ADD PRIMARY KEY (`id_aislamiento`),
  ADD UNIQUE KEY `id_aislamiento` (`id_aislamiento`),
  ADD KEY `id_incidente_gallina` (`id_incidente_gallina`),
  ADD KEY `id_galpon` (`id_galpon`);

--
-- Indices de la tabla `categoria_inventario`
--
ALTER TABLE `categoria_inventario`
  ADD PRIMARY KEY (`id_categoria`),
  ADD UNIQUE KEY `id_categoria` (`id_categoria`);

--
-- Indices de la tabla `detalle_huevos`
--
ALTER TABLE `detalle_huevos`
  ADD PRIMARY KEY (`id_detalle`),
  ADD UNIQUE KEY `id_detalle` (`id_detalle`),
  ADD KEY `id_producto` (`id_producto`),
  ADD KEY `id_venta` (`id_venta`);

--
-- Indices de la tabla `detalle_salvamento`
--
ALTER TABLE `detalle_salvamento`
  ADD PRIMARY KEY (`id_detalle`),
  ADD UNIQUE KEY `id_detalle` (`id_detalle`),
  ADD KEY `id_venta` (`id_venta`);

--
-- Indices de la tabla `fincas`
--
ALTER TABLE `fincas`
  ADD PRIMARY KEY (`id_finca`),
  ADD UNIQUE KEY `id_finca` (`id_finca`);

--
-- Indices de la tabla `galpones`
--
ALTER TABLE `galpones`
  ADD PRIMARY KEY (`id_galpon`),
  ADD UNIQUE KEY `id_galpon` (`id_galpon`),
  ADD KEY `id_finca` (`id_finca`);

--
-- Indices de la tabla `incidentes_gallina`
--
ALTER TABLE `incidentes_gallina`
  ADD PRIMARY KEY (`id_inc_gallina`),
  ADD UNIQUE KEY `id_inc_gallina` (`id_inc_gallina`),
  ADD KEY `galpon_origen` (`galpon_origen`);

--
-- Indices de la tabla `incidentes_generales`
--
ALTER TABLE `incidentes_generales`
  ADD PRIMARY KEY (`id_incidente`),
  ADD UNIQUE KEY `id_incidente` (`id_incidente`),
  ADD KEY `id_finca` (`id_finca`);

--
-- Indices de la tabla `ingreso_gallinas`
--
ALTER TABLE `ingreso_gallinas`
  ADD PRIMARY KEY (`id_ingreso`),
  ADD UNIQUE KEY `id_ingreso` (`id_ingreso`),
  ADD KEY `id_galpon` (`id_galpon`),
  ADD KEY `id_tipo_gallina` (`id_tipo_gallina`);

--
-- Indices de la tabla `inventario_finca`
--
ALTER TABLE `inventario_finca`
  ADD PRIMARY KEY (`id_inventario`),
  ADD UNIQUE KEY `id_inventario` (`id_inventario`),
  ADD KEY `id_categoria` (`id_categoria`),
  ADD KEY `id_finca` (`id_finca`);

--
-- Indices de la tabla `metodo_pago`
--
ALTER TABLE `metodo_pago`
  ADD PRIMARY KEY (`id_tipo`),
  ADD UNIQUE KEY `id_tipo` (`id_tipo`);

--
-- Indices de la tabla `modulos`
--
ALTER TABLE `modulos`
  ADD PRIMARY KEY (`id_modulo`),
  ADD UNIQUE KEY `id_modulo` (`id_modulo`);

--
-- Indices de la tabla `permisos`
--
ALTER TABLE `permisos`
  ADD PRIMARY KEY (`id_modulo`,`id_rol`),
  ADD KEY `id_rol` (`id_rol`);

--
-- Indices de la tabla `produccion_huevos`
--
ALTER TABLE `produccion_huevos`
  ADD PRIMARY KEY (`id_produccion`),
  ADD UNIQUE KEY `id_produccion` (`id_produccion`),
  ADD KEY `id_galpon` (`id_galpon`);

--
-- Indices de la tabla `registro_sensores`
--
ALTER TABLE `registro_sensores`
  ADD PRIMARY KEY (`id_registro`),
  ADD UNIQUE KEY `id_registro` (`id_registro`),
  ADD KEY `id_sensor` (`id_sensor`);

--
-- Indices de la tabla `roles`
--
ALTER TABLE `roles`
  ADD PRIMARY KEY (`id_rol`),
  ADD UNIQUE KEY `id_rol` (`id_rol`);

--
-- Indices de la tabla `salvamento`
--
ALTER TABLE `salvamento`
  ADD PRIMARY KEY (`id_salvamento`),
  ADD UNIQUE KEY `id_salvamento` (`id_salvamento`),
  ADD KEY `id_galpon` (`id_galpon`),
  ADD KEY `id_tipo_gallina` (`id_tipo_gallina`);

--
-- Indices de la tabla `sensores`
--
ALTER TABLE `sensores`
  ADD PRIMARY KEY (`id_sensor`),
  ADD UNIQUE KEY `id_sensor` (`id_sensor`),
  ADD KEY `id_tipo_sensor` (`id_tipo_sensor`),
  ADD KEY `id_galpon` (`id_galpon`);

--
-- Indices de la tabla `stock`
--
ALTER TABLE `stock`
  ADD PRIMARY KEY (`id_producto`),
  ADD UNIQUE KEY `id_producto` (`id_producto`),
  ADD KEY `id_produccion` (`id_produccion`);

--
-- Indices de la tabla `tareas`
--
ALTER TABLE `tareas`
  ADD PRIMARY KEY (`id_tarea`),
  ADD UNIQUE KEY `id_tarea` (`id_tarea`),
  ADD KEY `id_usuario` (`id_usuario`);

--
-- Indices de la tabla `tipo_gallinas`
--
ALTER TABLE `tipo_gallinas`
  ADD PRIMARY KEY (`id_tipo_gallinas`),
  ADD UNIQUE KEY `id_tipo_gallinas` (`id_tipo_gallinas`);

--
-- Indices de la tabla `tipo_sensores`
--
ALTER TABLE `tipo_sensores`
  ADD PRIMARY KEY (`id_tipo`),
  ADD UNIQUE KEY `id_tipo` (`id_tipo`);

--
-- Indices de la tabla `usuarios`
--
ALTER TABLE `usuarios`
  ADD PRIMARY KEY (`id_usuario`),
  ADD UNIQUE KEY `id_usuario` (`id_usuario`),
  ADD UNIQUE KEY `email` (`email`),
  ADD KEY `id_rol` (`id_rol`);

--
-- Indices de la tabla `ventas`
--
ALTER TABLE `ventas`
  ADD PRIMARY KEY (`id_venta`),
  ADD UNIQUE KEY `id_venta` (`id_venta`),
  ADD KEY `id_usuario` (`id_usuario`),
  ADD KEY `tipo_pago` (`tipo_pago`);

--
-- AUTO_INCREMENT de las tablas volcadas
--

--
-- AUTO_INCREMENT de la tabla `aislamiento`
--
ALTER TABLE `aislamiento`
  MODIFY `id_aislamiento` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `categoria_inventario`
--
ALTER TABLE `categoria_inventario`
  MODIFY `id_categoria` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `detalle_huevos`
--
ALTER TABLE `detalle_huevos`
  MODIFY `id_detalle` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `detalle_salvamento`
--
ALTER TABLE `detalle_salvamento`
  MODIFY `id_detalle` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `fincas`
--
ALTER TABLE `fincas`
  MODIFY `id_finca` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `galpones`
--
ALTER TABLE `galpones`
  MODIFY `id_galpon` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `incidentes_gallina`
--
ALTER TABLE `incidentes_gallina`
  MODIFY `id_inc_gallina` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `incidentes_generales`
--
ALTER TABLE `incidentes_generales`
  MODIFY `id_incidente` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `ingreso_gallinas`
--
ALTER TABLE `ingreso_gallinas`
  MODIFY `id_ingreso` smallint(5) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `inventario_finca`
--
ALTER TABLE `inventario_finca`
  MODIFY `id_inventario` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `metodo_pago`
--
ALTER TABLE `metodo_pago`
  MODIFY `id_tipo` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `modulos`
--
ALTER TABLE `modulos`
  MODIFY `id_modulo` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `produccion_huevos`
--
ALTER TABLE `produccion_huevos`
  MODIFY `id_produccion` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `registro_sensores`
--
ALTER TABLE `registro_sensores`
  MODIFY `id_registro` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `roles`
--
ALTER TABLE `roles`
  MODIFY `id_rol` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `salvamento`
--
ALTER TABLE `salvamento`
  MODIFY `id_salvamento` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `sensores`
--
ALTER TABLE `sensores`
  MODIFY `id_sensor` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `stock`
--
ALTER TABLE `stock`
  MODIFY `id_producto` smallint(5) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tareas`
--
ALTER TABLE `tareas`
  MODIFY `id_tarea` smallint(5) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tipo_gallinas`
--
ALTER TABLE `tipo_gallinas`
  MODIFY `id_tipo_gallinas` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tipo_sensores`
--
ALTER TABLE `tipo_sensores`
  MODIFY `id_tipo` tinyint(3) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `usuarios`
--
ALTER TABLE `usuarios`
  MODIFY `id_usuario` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `ventas`
--
ALTER TABLE `ventas`
  MODIFY `id_venta` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `aislamiento`
--
ALTER TABLE `aislamiento`
  ADD CONSTRAINT `aislamiento_ibfk_1` FOREIGN KEY (`id_incidente_gallina`) REFERENCES `incidentes_gallina` (`id_inc_gallina`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `aislamiento_ibfk_2` FOREIGN KEY (`id_galpon`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `detalle_huevos`
--
ALTER TABLE `detalle_huevos`
  ADD CONSTRAINT `detalle_huevos_ibfk_1` FOREIGN KEY (`id_producto`) REFERENCES `stock` (`id_producto`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `detalle_huevos_ibfk_2` FOREIGN KEY (`id_venta`) REFERENCES `ventas` (`id_venta`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `detalle_salvamento`
--
ALTER TABLE `detalle_salvamento`
  ADD CONSTRAINT `detalle_salvamento_ibfk_1` FOREIGN KEY (`id_venta`) REFERENCES `ventas` (`id_venta`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `galpones`
--
ALTER TABLE `galpones`
  ADD CONSTRAINT `galpones_ibfk_1` FOREIGN KEY (`id_finca`) REFERENCES `fincas` (`id_finca`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `incidentes_gallina`
--
ALTER TABLE `incidentes_gallina`
  ADD CONSTRAINT `incidentes_gallina_ibfk_1` FOREIGN KEY (`galpon_origen`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `incidentes_generales`
--
ALTER TABLE `incidentes_generales`
  ADD CONSTRAINT `incidentes_generales_ibfk_1` FOREIGN KEY (`id_finca`) REFERENCES `fincas` (`id_finca`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `ingreso_gallinas`
--
ALTER TABLE `ingreso_gallinas`
  ADD CONSTRAINT `ingreso_gallinas_ibfk_1` FOREIGN KEY (`id_galpon`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `ingreso_gallinas_ibfk_2` FOREIGN KEY (`id_tipo_gallina`) REFERENCES `tipo_gallinas` (`id_tipo_gallinas`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `inventario_finca`
--
ALTER TABLE `inventario_finca`
  ADD CONSTRAINT `inventario_finca_ibfk_1` FOREIGN KEY (`id_categoria`) REFERENCES `categoria_inventario` (`id_categoria`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `inventario_finca_ibfk_2` FOREIGN KEY (`id_finca`) REFERENCES `fincas` (`id_finca`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `permisos`
--
ALTER TABLE `permisos`
  ADD CONSTRAINT `permisos_ibfk_1` FOREIGN KEY (`id_modulo`) REFERENCES `modulos` (`id_modulo`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `permisos_ibfk_2` FOREIGN KEY (`id_rol`) REFERENCES `roles` (`id_rol`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `produccion_huevos`
--
ALTER TABLE `produccion_huevos`
  ADD CONSTRAINT `produccion_huevos_ibfk_1` FOREIGN KEY (`id_galpon`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `registro_sensores`
--
ALTER TABLE `registro_sensores`
  ADD CONSTRAINT `registro_sensores_ibfk_1` FOREIGN KEY (`id_sensor`) REFERENCES `sensores` (`id_sensor`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `salvamento`
--
ALTER TABLE `salvamento`
  ADD CONSTRAINT `salvamento_ibfk_1` FOREIGN KEY (`id_galpon`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `salvamento_ibfk_2` FOREIGN KEY (`id_tipo_gallina`) REFERENCES `tipo_gallinas` (`id_tipo_gallinas`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `sensores`
--
ALTER TABLE `sensores`
  ADD CONSTRAINT `sensores_ibfk_1` FOREIGN KEY (`id_tipo_sensor`) REFERENCES `tipo_sensores` (`id_tipo`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `sensores_ibfk_2` FOREIGN KEY (`id_galpon`) REFERENCES `galpones` (`id_galpon`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `stock`
--
ALTER TABLE `stock`
  ADD CONSTRAINT `stock_ibfk_1` FOREIGN KEY (`id_produccion`) REFERENCES `produccion_huevos` (`id_produccion`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `tareas`
--
ALTER TABLE `tareas`
  ADD CONSTRAINT `tareas_ibfk_1` FOREIGN KEY (`id_usuario`) REFERENCES `usuarios` (`id_usuario`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `usuarios`
--
ALTER TABLE `usuarios`
  ADD CONSTRAINT `usuarios_ibfk_1` FOREIGN KEY (`id_rol`) REFERENCES `roles` (`id_rol`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Filtros para la tabla `ventas`
--
ALTER TABLE `ventas`
  ADD CONSTRAINT `ventas_ibfk_1` FOREIGN KEY (`id_usuario`) REFERENCES `usuarios` (`id_usuario`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `ventas_ibfk_2` FOREIGN KEY (`tipo_pago`) REFERENCES `metodo_pago` (`id_tipo`) ON DELETE NO ACTION ON UPDATE NO ACTION;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
