'use strict';

const express = require('express');

function findRoute(app, method, routePath) {
  const stack = app?._router?.stack || [];
  return stack.find((layer) => layer.route
    && layer.route.path === routePath
    && Boolean(layer.route.methods?.[String(method).toLowerCase()]));
}

function handlerLayer(method, routePath, handler) {
  const router = express.Router();
  router[String(method).toLowerCase()](routePath, handler);
  return router.stack[0].route.stack[0];
}

function prepend(routeLayer, method, routePath, handler) {
  routeLayer.route.stack.unshift(handlerLayer(method, routePath, handler));
}

function insertBeforeLast(routeLayer, method, routePath, handler) {
  const stack = routeLayer.route.stack;
  stack.splice(Math.max(0, stack.length - 1), 0, handlerLayer(method, routePath, handler));
}

function replaceLast(routeLayer, handler) {
  const stack = routeLayer.route.stack;
  if (!stack.length) throw new Error('route_has_no_handlers');
  stack[stack.length - 1].handle = handler;
  stack[stack.length - 1].name = handler.name || 'coreHandler';
}

function replaceAfterFirst(routeLayer, method, routePath, handlers) {
  const first = routeLayer.route.stack[0];
  routeLayer.route.stack = [first, ...handlers.map((handler) => handlerLayer(method, routePath, handler))];
}

function insertRouterBeforeFallback(app, router) {
  const stack = app?._router?.stack;
  if (!stack) throw new Error('express_router_not_initialized');
  let index = stack.findIndex((layer) => layer.route?.path === '*');
  if (index < 0) index = stack.findIndex((layer) => typeof layer.handle === 'function' && layer.handle.length === 4);
  if (index < 0) index = stack.length;
  stack.splice(index, 0, ...router.stack);
}

module.exports = { findRoute, handlerLayer, prepend, insertBeforeLast, replaceLast, replaceAfterFirst, insertRouterBeforeFallback };
