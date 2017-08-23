(function webpackUniversalModuleDefinition(root, factory) {
	if(typeof exports === 'object' && typeof module === 'object')
		module.exports = factory(require("echarts"));
	else if(typeof define === 'function' && define.amd)
		define(["echarts"], factory);
	else if(typeof exports === 'object')
		exports["echarts-wordcloud"] = factory(require("echarts"));
	else
		root["echarts-wordcloud"] = factory(root["echarts"]);
})(this, function(__WEBPACK_EXTERNAL_MODULE_7__) {
return /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, {
/******/ 				configurable: false,
/******/ 				enumerable: true,
/******/ 				get: getter
/******/ 			});
/******/ 		}
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 22);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports) {

/**
 * @module zrender/core/util
 */


    // 用于处理merge时无法遍历Date等对象的问题
    var BUILTIN_OBJECT = {
        '[object Function]': 1,
        '[object RegExp]': 1,
        '[object Date]': 1,
        '[object Error]': 1,
        '[object CanvasGradient]': 1,
        '[object CanvasPattern]': 1,
        // For node-canvas
        '[object Image]': 1,
        '[object Canvas]': 1
    };

    var TYPED_ARRAY = {
        '[object Int8Array]': 1,
        '[object Uint8Array]': 1,
        '[object Uint8ClampedArray]': 1,
        '[object Int16Array]': 1,
        '[object Uint16Array]': 1,
        '[object Int32Array]': 1,
        '[object Uint32Array]': 1,
        '[object Float32Array]': 1,
        '[object Float64Array]': 1
    };

    var objToString = Object.prototype.toString;

    var arrayProto = Array.prototype;
    var nativeForEach = arrayProto.forEach;
    var nativeFilter = arrayProto.filter;
    var nativeSlice = arrayProto.slice;
    var nativeMap = arrayProto.map;
    var nativeReduce = arrayProto.reduce;

    /**
     * Those data types can be cloned:
     *     Plain object, Array, TypedArray, number, string, null, undefined.
     * Those data types will be assgined using the orginal data:
     *     BUILTIN_OBJECT
     * Instance of user defined class will be cloned to a plain object, without
     * properties in prototype.
     * Other data types is not supported (not sure what will happen).
     *
     * Caution: do not support clone Date, for performance consideration.
     * (There might be a large number of date in `series.data`).
     * So date should not be modified in and out of echarts.
     *
     * @param {*} source
     * @return {*} new
     */
    function clone(source) {
        if (source == null || typeof source != 'object') {
            return source;
        }

        var result = source;
        var typeStr = objToString.call(source);

        if (typeStr === '[object Array]') {
            result = [];
            for (var i = 0, len = source.length; i < len; i++) {
                result[i] = clone(source[i]);
            }
        }
        else if (TYPED_ARRAY[typeStr]) {
            result = source.constructor.from(source);
        }
        else if (!BUILTIN_OBJECT[typeStr] && !isPrimitive(source) && !isDom(source)) {
            result = {};
            for (var key in source) {
                if (source.hasOwnProperty(key)) {
                    result[key] = clone(source[key]);
                }
            }
        }

        return result;
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} target
     * @param {*} source
     * @param {boolean} [overwrite=false]
     */
    function merge(target, source, overwrite) {
        // We should escapse that source is string
        // and enter for ... in ...
        if (!isObject(source) || !isObject(target)) {
            return overwrite ? clone(source) : target;
        }

        for (var key in source) {
            if (source.hasOwnProperty(key)) {
                var targetProp = target[key];
                var sourceProp = source[key];

                if (isObject(sourceProp)
                    && isObject(targetProp)
                    && !isArray(sourceProp)
                    && !isArray(targetProp)
                    && !isDom(sourceProp)
                    && !isDom(targetProp)
                    && !isBuiltInObject(sourceProp)
                    && !isBuiltInObject(targetProp)
                    && !isPrimitive(sourceProp)
                    && !isPrimitive(targetProp)
                ) {
                    // 如果需要递归覆盖，就递归调用merge
                    merge(targetProp, sourceProp, overwrite);
                }
                else if (overwrite || !(key in target)) {
                    // 否则只处理overwrite为true，或者在目标对象中没有此属性的情况
                    // NOTE，在 target[key] 不存在的时候也是直接覆盖
                    target[key] = clone(source[key], true);
                }
            }
        }

        return target;
    }

    /**
     * @param {Array} targetAndSources The first item is target, and the rests are source.
     * @param {boolean} [overwrite=false]
     * @return {*} target
     */
    function mergeAll(targetAndSources, overwrite) {
        var result = targetAndSources[0];
        for (var i = 1, len = targetAndSources.length; i < len; i++) {
            result = merge(result, targetAndSources[i], overwrite);
        }
        return result;
    }

    /**
     * @param {*} target
     * @param {*} source
     * @memberOf module:zrender/core/util
     */
    function extend(target, source) {
        for (var key in source) {
            if (source.hasOwnProperty(key)) {
                target[key] = source[key];
            }
        }
        return target;
    }

    /**
     * @param {*} target
     * @param {*} source
     * @param {boolen} [overlay=false]
     * @memberOf module:zrender/core/util
     */
    function defaults(target, source, overlay) {
        for (var key in source) {
            if (source.hasOwnProperty(key)
                && (overlay ? source[key] != null : target[key] == null)
            ) {
                target[key] = source[key];
            }
        }
        return target;
    }

    function createCanvas() {
        return document.createElement('canvas');
    }
    // FIXME
    var _ctx;
    function getContext() {
        if (!_ctx) {
            // Use util.createCanvas instead of createCanvas
            // because createCanvas may be overwritten in different environment
            _ctx = util.createCanvas().getContext('2d');
        }
        return _ctx;
    }

    /**
     * 查询数组中元素的index
     * @memberOf module:zrender/core/util
     */
    function indexOf(array, value) {
        if (array) {
            if (array.indexOf) {
                return array.indexOf(value);
            }
            for (var i = 0, len = array.length; i < len; i++) {
                if (array[i] === value) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * 构造类继承关系
     *
     * @memberOf module:zrender/core/util
     * @param {Function} clazz 源类
     * @param {Function} baseClazz 基类
     */
    function inherits(clazz, baseClazz) {
        var clazzPrototype = clazz.prototype;
        function F() {}
        F.prototype = baseClazz.prototype;
        clazz.prototype = new F();

        for (var prop in clazzPrototype) {
            clazz.prototype[prop] = clazzPrototype[prop];
        }
        clazz.prototype.constructor = clazz;
        clazz.superClass = baseClazz;
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {Object|Function} target
     * @param {Object|Function} sorce
     * @param {boolean} overlay
     */
    function mixin(target, source, overlay) {
        target = 'prototype' in target ? target.prototype : target;
        source = 'prototype' in source ? source.prototype : source;

        defaults(target, source, overlay);
    }

    /**
     * Consider typed array.
     * @param {Array|TypedArray} data
     */
    function isArrayLike(data) {
        if (! data) {
            return;
        }
        if (typeof data == 'string') {
            return false;
        }
        return typeof data.length == 'number';
    }

    /**
     * 数组或对象遍历
     * @memberOf module:zrender/core/util
     * @param {Object|Array} obj
     * @param {Function} cb
     * @param {*} [context]
     */
    function each(obj, cb, context) {
        if (!(obj && cb)) {
            return;
        }
        if (obj.forEach && obj.forEach === nativeForEach) {
            obj.forEach(cb, context);
        }
        else if (obj.length === +obj.length) {
            for (var i = 0, len = obj.length; i < len; i++) {
                cb.call(context, obj[i], i, obj);
            }
        }
        else {
            for (var key in obj) {
                if (obj.hasOwnProperty(key)) {
                    cb.call(context, obj[key], key, obj);
                }
            }
        }
    }

    /**
     * 数组映射
     * @memberOf module:zrender/core/util
     * @param {Array} obj
     * @param {Function} cb
     * @param {*} [context]
     * @return {Array}
     */
    function map(obj, cb, context) {
        if (!(obj && cb)) {
            return;
        }
        if (obj.map && obj.map === nativeMap) {
            return obj.map(cb, context);
        }
        else {
            var result = [];
            for (var i = 0, len = obj.length; i < len; i++) {
                result.push(cb.call(context, obj[i], i, obj));
            }
            return result;
        }
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {Array} obj
     * @param {Function} cb
     * @param {Object} [memo]
     * @param {*} [context]
     * @return {Array}
     */
    function reduce(obj, cb, memo, context) {
        if (!(obj && cb)) {
            return;
        }
        if (obj.reduce && obj.reduce === nativeReduce) {
            return obj.reduce(cb, memo, context);
        }
        else {
            for (var i = 0, len = obj.length; i < len; i++) {
                memo = cb.call(context, memo, obj[i], i, obj);
            }
            return memo;
        }
    }

    /**
     * 数组过滤
     * @memberOf module:zrender/core/util
     * @param {Array} obj
     * @param {Function} cb
     * @param {*} [context]
     * @return {Array}
     */
    function filter(obj, cb, context) {
        if (!(obj && cb)) {
            return;
        }
        if (obj.filter && obj.filter === nativeFilter) {
            return obj.filter(cb, context);
        }
        else {
            var result = [];
            for (var i = 0, len = obj.length; i < len; i++) {
                if (cb.call(context, obj[i], i, obj)) {
                    result.push(obj[i]);
                }
            }
            return result;
        }
    }

    /**
     * 数组项查找
     * @memberOf module:zrender/core/util
     * @param {Array} obj
     * @param {Function} cb
     * @param {*} [context]
     * @return {Array}
     */
    function find(obj, cb, context) {
        if (!(obj && cb)) {
            return;
        }
        for (var i = 0, len = obj.length; i < len; i++) {
            if (cb.call(context, obj[i], i, obj)) {
                return obj[i];
            }
        }
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {Function} func
     * @param {*} context
     * @return {Function}
     */
    function bind(func, context) {
        var args = nativeSlice.call(arguments, 2);
        return function () {
            return func.apply(context, args.concat(nativeSlice.call(arguments)));
        };
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {Function} func
     * @return {Function}
     */
    function curry(func) {
        var args = nativeSlice.call(arguments, 1);
        return function () {
            return func.apply(this, args.concat(nativeSlice.call(arguments)));
        };
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isArray(value) {
        return objToString.call(value) === '[object Array]';
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isFunction(value) {
        return typeof value === 'function';
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isString(value) {
        return objToString.call(value) === '[object String]';
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isObject(value) {
        // Avoid a V8 JIT bug in Chrome 19-20.
        // See https://code.google.com/p/v8/issues/detail?id=2291 for more details.
        var type = typeof value;
        return type === 'function' || (!!value && type == 'object');
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isBuiltInObject(value) {
        return !!BUILTIN_OBJECT[objToString.call(value)];
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {*} value
     * @return {boolean}
     */
    function isDom(value) {
        return typeof value === 'object'
            && typeof value.nodeType === 'number'
            && typeof value.ownerDocument === 'object';
    }

    /**
     * Whether is exactly NaN. Notice isNaN('a') returns true.
     * @param {*} value
     * @return {boolean}
     */
    function eqNaN(value) {
        return value !== value;
    }

    /**
     * If value1 is not null, then return value1, otherwise judget rest of values.
     * @memberOf module:zrender/core/util
     * @return {*} Final value
     */
    function retrieve(values) {
        for (var i = 0, len = arguments.length; i < len; i++) {
            if (arguments[i] != null) {
                return arguments[i];
            }
        }
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {Array} arr
     * @param {number} startIndex
     * @param {number} endIndex
     * @return {Array}
     */
    function slice() {
        return Function.call.apply(nativeSlice, arguments);
    }

    /**
     * @memberOf module:zrender/core/util
     * @param {boolean} condition
     * @param {string} message
     */
    function assert(condition, message) {
        if (!condition) {
            throw new Error(message);
        }
    }

    var primitiveKey = '__ec_primitive__';
    /**
     * Set an object as primitive to be ignored traversing children in clone or merge
     */
    function setAsPrimitive(obj) {
        obj[primitiveKey] = true;
    }

    function isPrimitive(obj) {
        return obj[primitiveKey];
    }

    /**
     * @constructor
     * @param {Object} obj Only apply `ownProperty`.
     */
    function HashMap(obj) {
        obj && each(obj, function (value, key) {
            this.set(key, value);
        }, this);
    }

    // Add prefix to avoid conflict with Object.prototype.
    var HASH_MAP_PREFIX = '_ec_';
    var HASH_MAP_PREFIX_LENGTH = 4;

    HashMap.prototype = {
        constructor: HashMap,
        // Do not provide `has` method to avoid defining what is `has`.
        // (We usually treat `null` and `undefined` as the same, different
        // from ES6 Map).
        get: function (key) {
            return this[HASH_MAP_PREFIX + key];
        },
        set: function (key, value) {
            this[HASH_MAP_PREFIX + key] = value;
            // Comparing with invocation chaining, `return value` is more commonly
            // used in this case: `var someVal = map.set('a', genVal());`
            return value;
        },
        // Although util.each can be performed on this hashMap directly, user
        // should not use the exposed keys, who are prefixed.
        each: function (cb, context) {
            context !== void 0 && (cb = bind(cb, context));
            for (var prefixedKey in this) {
                this.hasOwnProperty(prefixedKey)
                    && cb(this[prefixedKey], prefixedKey.slice(HASH_MAP_PREFIX_LENGTH));
            }
        },
        // Do not use this method if performance sensitive.
        removeKey: function (key) {
            delete this[key];
        }
    };

    function createHashMap(obj) {
        return new HashMap(obj);
    }

    var util = {
        inherits: inherits,
        mixin: mixin,
        clone: clone,
        merge: merge,
        mergeAll: mergeAll,
        extend: extend,
        defaults: defaults,
        getContext: getContext,
        createCanvas: createCanvas,
        indexOf: indexOf,
        slice: slice,
        find: find,
        isArrayLike: isArrayLike,
        each: each,
        map: map,
        reduce: reduce,
        filter: filter,
        bind: bind,
        curry: curry,
        isArray: isArray,
        isString: isString,
        isObject: isObject,
        isFunction: isFunction,
        isBuiltInObject: isBuiltInObject,
        isDom: isDom,
        eqNaN: eqNaN,
        retrieve: retrieve,
        assert: assert,
        setAsPrimitive: setAsPrimitive,
        createHashMap: createHashMap,
        noop: function () {}
    };
    module.exports = util;



/***/ }),
/* 1 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Path element
 * @module zrender/graphic/Path
 */



    var Displayable = __webpack_require__(11);
    var zrUtil = __webpack_require__(0);
    var PathProxy = __webpack_require__(6);
    var pathContain = __webpack_require__(45);

    var Pattern = __webpack_require__(51);
    var getCanvasPattern = Pattern.prototype.getCanvasPattern;

    var abs = Math.abs;

    var pathProxyForDraw = new PathProxy(true);
    /**
     * @alias module:zrender/graphic/Path
     * @extends module:zrender/graphic/Displayable
     * @constructor
     * @param {Object} opts
     */
    function Path(opts) {
        Displayable.call(this, opts);

        /**
         * @type {module:zrender/core/PathProxy}
         * @readOnly
         */
        this.path = null;
    }

    Path.prototype = {

        constructor: Path,

        type: 'path',

        __dirtyPath: true,

        strokeContainThreshold: 5,

        brush: function (ctx, prevEl) {
            var style = this.style;
            var path = this.path || pathProxyForDraw;
            var hasStroke = style.hasStroke();
            var hasFill = style.hasFill();
            var fill = style.fill;
            var stroke = style.stroke;
            var hasFillGradient = hasFill && !!(fill.colorStops);
            var hasStrokeGradient = hasStroke && !!(stroke.colorStops);
            var hasFillPattern = hasFill && !!(fill.image);
            var hasStrokePattern = hasStroke && !!(stroke.image);

            style.bind(ctx, this, prevEl);
            this.setTransform(ctx);

            if (this.__dirty) {
                var rect;
                // Update gradient because bounding rect may changed
                if (hasFillGradient) {
                    rect = rect || this.getBoundingRect();
                    this._fillGradient = style.getGradient(ctx, fill, rect);
                }
                if (hasStrokeGradient) {
                    rect = rect || this.getBoundingRect();
                    this._strokeGradient = style.getGradient(ctx, stroke, rect);
                }
            }
            // Use the gradient or pattern
            if (hasFillGradient) {
                // PENDING If may have affect the state
                ctx.fillStyle = this._fillGradient;
            }
            else if (hasFillPattern) {
                ctx.fillStyle = getCanvasPattern.call(fill, ctx);
            }
            if (hasStrokeGradient) {
                ctx.strokeStyle = this._strokeGradient;
            }
            else if (hasStrokePattern) {
                ctx.strokeStyle = getCanvasPattern.call(stroke, ctx);
            }

            var lineDash = style.lineDash;
            var lineDashOffset = style.lineDashOffset;

            var ctxLineDash = !!ctx.setLineDash;

            // Update path sx, sy
            var scale = this.getGlobalScale();
            path.setScale(scale[0], scale[1]);

            // Proxy context
            // Rebuild path in following 2 cases
            // 1. Path is dirty
            // 2. Path needs javascript implemented lineDash stroking.
            //    In this case, lineDash information will not be saved in PathProxy
            if (this.__dirtyPath
                || (lineDash && !ctxLineDash && hasStroke)
            ) {
                path.beginPath(ctx);

                // Setting line dash before build path
                if (lineDash && !ctxLineDash) {
                    path.setLineDash(lineDash);
                    path.setLineDashOffset(lineDashOffset);
                }

                this.buildPath(path, this.shape, false);

                // Clear path dirty flag
                if (this.path) {
                    this.__dirtyPath = false;
                }
            }
            else {
                // Replay path building
                ctx.beginPath();
                this.path.rebuildPath(ctx);
            }

            hasFill && path.fill(ctx);

            if (lineDash && ctxLineDash) {
                ctx.setLineDash(lineDash);
                ctx.lineDashOffset = lineDashOffset;
            }

            hasStroke && path.stroke(ctx);

            if (lineDash && ctxLineDash) {
                // PENDING
                // Remove lineDash
                ctx.setLineDash([]);
            }


            this.restoreTransform(ctx);

            // Draw rect text
            if (style.text != null) {
                this.drawRectText(ctx, this.getBoundingRect());
            }
        },

        // When bundling path, some shape may decide if use moveTo to begin a new subpath or closePath
        // Like in circle
        buildPath: function (ctx, shapeCfg, inBundle) {},

        createPathProxy: function () {
            this.path = new PathProxy();
        },

        getBoundingRect: function () {
            var rect = this._rect;
            var style = this.style;
            var needsUpdateRect = !rect;
            if (needsUpdateRect) {
                var path = this.path;
                if (!path) {
                    // Create path on demand.
                    path = this.path = new PathProxy();
                }
                if (this.__dirtyPath) {
                    path.beginPath();
                    this.buildPath(path, this.shape, false);
                }
                rect = path.getBoundingRect();
            }
            this._rect = rect;

            if (style.hasStroke()) {
                // Needs update rect with stroke lineWidth when
                // 1. Element changes scale or lineWidth
                // 2. Shape is changed
                var rectWithStroke = this._rectWithStroke || (this._rectWithStroke = rect.clone());
                if (this.__dirty || needsUpdateRect) {
                    rectWithStroke.copy(rect);
                    // FIXME Must after updateTransform
                    var w = style.lineWidth;
                    // PENDING, Min line width is needed when line is horizontal or vertical
                    var lineScale = style.strokeNoScale ? this.getLineScale() : 1;

                    // Only add extra hover lineWidth when there are no fill
                    if (!style.hasFill()) {
                        w = Math.max(w, this.strokeContainThreshold || 4);
                    }
                    // Consider line width
                    // Line scale can't be 0;
                    if (lineScale > 1e-10) {
                        rectWithStroke.width += w / lineScale;
                        rectWithStroke.height += w / lineScale;
                        rectWithStroke.x -= w / lineScale / 2;
                        rectWithStroke.y -= w / lineScale / 2;
                    }
                }

                // Return rect with stroke
                return rectWithStroke;
            }

            return rect;
        },

        contain: function (x, y) {
            var localPos = this.transformCoordToLocal(x, y);
            var rect = this.getBoundingRect();
            var style = this.style;
            x = localPos[0];
            y = localPos[1];

            if (rect.contain(x, y)) {
                var pathData = this.path.data;
                if (style.hasStroke()) {
                    var lineWidth = style.lineWidth;
                    var lineScale = style.strokeNoScale ? this.getLineScale() : 1;
                    // Line scale can't be 0;
                    if (lineScale > 1e-10) {
                        // Only add extra hover lineWidth when there are no fill
                        if (!style.hasFill()) {
                            lineWidth = Math.max(lineWidth, this.strokeContainThreshold);
                        }
                        if (pathContain.containStroke(
                            pathData, lineWidth / lineScale, x, y
                        )) {
                            return true;
                        }
                    }
                }
                if (style.hasFill()) {
                    return pathContain.contain(pathData, x, y);
                }
            }
            return false;
        },

        /**
         * @param  {boolean} dirtyPath
         */
        dirty: function (dirtyPath) {
            if (dirtyPath == null) {
                dirtyPath = true;
            }
            // Only mark dirty, not mark clean
            if (dirtyPath) {
                this.__dirtyPath = dirtyPath;
                this._rect = null;
            }

            this.__dirty = true;

            this.__zr && this.__zr.refresh();

            // Used as a clipping path
            if (this.__clipTarget) {
                this.__clipTarget.dirty();
            }
        },

        /**
         * Alias for animate('shape')
         * @param {boolean} loop
         */
        animateShape: function (loop) {
            return this.animate('shape', loop);
        },

        // Overwrite attrKV
        attrKV: function (key, value) {
            // FIXME
            if (key === 'shape') {
                this.setShape(value);
                this.__dirtyPath = true;
                this._rect = null;
            }
            else {
                Displayable.prototype.attrKV.call(this, key, value);
            }
        },

        /**
         * @param {Object|string} key
         * @param {*} value
         */
        setShape: function (key, value) {
            var shape = this.shape;
            // Path from string may not have shape
            if (shape) {
                if (zrUtil.isObject(key)) {
                    for (var name in key) {
                        if (key.hasOwnProperty(name)) {
                            shape[name] = key[name];
                        }
                    }
                }
                else {
                    shape[key] = value;
                }
                this.dirty(true);
            }
            return this;
        },

        getLineScale: function () {
            var m = this.transform;
            // Get the line scale.
            // Determinant of `m` means how much the area is enlarged by the
            // transformation. So its square root can be used as a scale factor
            // for width.
            return m && abs(m[0] - 1) > 1e-10 && abs(m[3] - 1) > 1e-10
                ? Math.sqrt(abs(m[0] * m[3] - m[2] * m[1]))
                : 1;
        }
    };

    /**
     * 扩展一个 Path element, 比如星形，圆等。
     * Extend a path element
     * @param {Object} props
     * @param {string} props.type Path type
     * @param {Function} props.init Initialize
     * @param {Function} props.buildPath Overwrite buildPath method
     * @param {Object} [props.style] Extended default style config
     * @param {Object} [props.shape] Extended default shape config
     */
    Path.extend = function (defaults) {
        var Sub = function (opts) {
            Path.call(this, opts);

            if (defaults.style) {
                // Extend default style
                this.style.extendFrom(defaults.style, false);
            }

            // Extend default shape
            var defaultShape = defaults.shape;
            if (defaultShape) {
                this.shape = this.shape || {};
                var thisShape = this.shape;
                for (var name in defaultShape) {
                    if (
                        ! thisShape.hasOwnProperty(name)
                        && defaultShape.hasOwnProperty(name)
                    ) {
                        thisShape[name] = defaultShape[name];
                    }
                }
            }

            defaults.init && defaults.init.call(this, opts);
        };

        zrUtil.inherits(Sub, Path);

        // FIXME 不能 extend position, rotation 等引用对象
        for (var name in defaults) {
            // Extending prototype values and methods
            if (name !== 'style' && name !== 'shape') {
                Sub.prototype[name] = defaults[name];
            }
        }

        return Sub;
    };

    zrUtil.inherits(Path, Displayable);

    module.exports = Path;


/***/ }),
/* 2 */
/***/ (function(module, exports) {


    var ArrayCtor = typeof Float32Array === 'undefined'
        ? Array
        : Float32Array;

    /**
     * @typedef {Float32Array|Array.<number>} Vector2
     */
    /**
     * 二维向量类
     * @exports zrender/tool/vector
     */
    var vector = {
        /**
         * 创建一个向量
         * @param {number} [x=0]
         * @param {number} [y=0]
         * @return {Vector2}
         */
        create: function (x, y) {
            var out = new ArrayCtor(2);
            if (x == null) {
                x = 0;
            }
            if (y == null) {
                y = 0;
            }
            out[0] = x;
            out[1] = y;
            return out;
        },

        /**
         * 复制向量数据
         * @param {Vector2} out
         * @param {Vector2} v
         * @return {Vector2}
         */
        copy: function (out, v) {
            out[0] = v[0];
            out[1] = v[1];
            return out;
        },

        /**
         * 克隆一个向量
         * @param {Vector2} v
         * @return {Vector2}
         */
        clone: function (v) {
            var out = new ArrayCtor(2);
            out[0] = v[0];
            out[1] = v[1];
            return out;
        },

        /**
         * 设置向量的两个项
         * @param {Vector2} out
         * @param {number} a
         * @param {number} b
         * @return {Vector2} 结果
         */
        set: function (out, a, b) {
            out[0] = a;
            out[1] = b;
            return out;
        },

        /**
         * 向量相加
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         */
        add: function (out, v1, v2) {
            out[0] = v1[0] + v2[0];
            out[1] = v1[1] + v2[1];
            return out;
        },

        /**
         * 向量缩放后相加
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         * @param {number} a
         */
        scaleAndAdd: function (out, v1, v2, a) {
            out[0] = v1[0] + v2[0] * a;
            out[1] = v1[1] + v2[1] * a;
            return out;
        },

        /**
         * 向量相减
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         */
        sub: function (out, v1, v2) {
            out[0] = v1[0] - v2[0];
            out[1] = v1[1] - v2[1];
            return out;
        },

        /**
         * 向量长度
         * @param {Vector2} v
         * @return {number}
         */
        len: function (v) {
            return Math.sqrt(this.lenSquare(v));
        },

        /**
         * 向量长度平方
         * @param {Vector2} v
         * @return {number}
         */
        lenSquare: function (v) {
            return v[0] * v[0] + v[1] * v[1];
        },

        /**
         * 向量乘法
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         */
        mul: function (out, v1, v2) {
            out[0] = v1[0] * v2[0];
            out[1] = v1[1] * v2[1];
            return out;
        },

        /**
         * 向量除法
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         */
        div: function (out, v1, v2) {
            out[0] = v1[0] / v2[0];
            out[1] = v1[1] / v2[1];
            return out;
        },

        /**
         * 向量点乘
         * @param {Vector2} v1
         * @param {Vector2} v2
         * @return {number}
         */
        dot: function (v1, v2) {
            return v1[0] * v2[0] + v1[1] * v2[1];
        },

        /**
         * 向量缩放
         * @param {Vector2} out
         * @param {Vector2} v
         * @param {number} s
         */
        scale: function (out, v, s) {
            out[0] = v[0] * s;
            out[1] = v[1] * s;
            return out;
        },

        /**
         * 向量归一化
         * @param {Vector2} out
         * @param {Vector2} v
         */
        normalize: function (out, v) {
            var d = vector.len(v);
            if (d === 0) {
                out[0] = 0;
                out[1] = 0;
            }
            else {
                out[0] = v[0] / d;
                out[1] = v[1] / d;
            }
            return out;
        },

        /**
         * 计算向量间距离
         * @param {Vector2} v1
         * @param {Vector2} v2
         * @return {number}
         */
        distance: function (v1, v2) {
            return Math.sqrt(
                (v1[0] - v2[0]) * (v1[0] - v2[0])
                + (v1[1] - v2[1]) * (v1[1] - v2[1])
            );
        },

        /**
         * 向量距离平方
         * @param {Vector2} v1
         * @param {Vector2} v2
         * @return {number}
         */
        distanceSquare: function (v1, v2) {
            return (v1[0] - v2[0]) * (v1[0] - v2[0])
                + (v1[1] - v2[1]) * (v1[1] - v2[1]);
        },

        /**
         * 求负向量
         * @param {Vector2} out
         * @param {Vector2} v
         */
        negate: function (out, v) {
            out[0] = -v[0];
            out[1] = -v[1];
            return out;
        },

        /**
         * 插值两个点
         * @param {Vector2} out
         * @param {Vector2} v1
         * @param {Vector2} v2
         * @param {number} t
         */
        lerp: function (out, v1, v2, t) {
            out[0] = v1[0] + t * (v2[0] - v1[0]);
            out[1] = v1[1] + t * (v2[1] - v1[1]);
            return out;
        },

        /**
         * 矩阵左乘向量
         * @param {Vector2} out
         * @param {Vector2} v
         * @param {Vector2} m
         */
        applyTransform: function (out, v, m) {
            var x = v[0];
            var y = v[1];
            out[0] = m[0] * x + m[2] * y + m[4];
            out[1] = m[1] * x + m[3] * y + m[5];
            return out;
        },
        /**
         * 求两个向量最小值
         * @param  {Vector2} out
         * @param  {Vector2} v1
         * @param  {Vector2} v2
         */
        min: function (out, v1, v2) {
            out[0] = Math.min(v1[0], v2[0]);
            out[1] = Math.min(v1[1], v2[1]);
            return out;
        },
        /**
         * 求两个向量最大值
         * @param  {Vector2} out
         * @param  {Vector2} v1
         * @param  {Vector2} v2
         */
        max: function (out, v1, v2) {
            out[0] = Math.max(v1[0], v2[0]);
            out[1] = Math.max(v1[1], v2[1]);
            return out;
        }
    };

    vector.length = vector.len;
    vector.lengthSquare = vector.lenSquare;
    vector.dist = vector.distance;
    vector.distSquare = vector.distanceSquare;

    module.exports = vector;



/***/ }),
/* 3 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * @module echarts/core/BoundingRect
 */


    var vec2 = __webpack_require__(2);
    var matrix = __webpack_require__(8);

    var v2ApplyTransform = vec2.applyTransform;
    var mathMin = Math.min;
    var mathMax = Math.max;
    /**
     * @alias module:echarts/core/BoundingRect
     */
    function BoundingRect(x, y, width, height) {

        if (width < 0) {
            x = x + width;
            width = -width;
        }
        if (height < 0) {
            y = y + height;
            height = -height;
        }

        /**
         * @type {number}
         */
        this.x = x;
        /**
         * @type {number}
         */
        this.y = y;
        /**
         * @type {number}
         */
        this.width = width;
        /**
         * @type {number}
         */
        this.height = height;
    }

    BoundingRect.prototype = {

        constructor: BoundingRect,

        /**
         * @param {module:echarts/core/BoundingRect} other
         */
        union: function (other) {
            var x = mathMin(other.x, this.x);
            var y = mathMin(other.y, this.y);

            this.width = mathMax(
                    other.x + other.width,
                    this.x + this.width
                ) - x;
            this.height = mathMax(
                    other.y + other.height,
                    this.y + this.height
                ) - y;
            this.x = x;
            this.y = y;
        },

        /**
         * @param {Array.<number>} m
         * @methods
         */
        applyTransform: (function () {
            var lt = [];
            var rb = [];
            var lb = [];
            var rt = [];
            return function (m) {
                // In case usage like this
                // el.getBoundingRect().applyTransform(el.transform)
                // And element has no transform
                if (!m) {
                    return;
                }
                lt[0] = lb[0] = this.x;
                lt[1] = rt[1] = this.y;
                rb[0] = rt[0] = this.x + this.width;
                rb[1] = lb[1] = this.y + this.height;

                v2ApplyTransform(lt, lt, m);
                v2ApplyTransform(rb, rb, m);
                v2ApplyTransform(lb, lb, m);
                v2ApplyTransform(rt, rt, m);

                this.x = mathMin(lt[0], rb[0], lb[0], rt[0]);
                this.y = mathMin(lt[1], rb[1], lb[1], rt[1]);
                var maxX = mathMax(lt[0], rb[0], lb[0], rt[0]);
                var maxY = mathMax(lt[1], rb[1], lb[1], rt[1]);
                this.width = maxX - this.x;
                this.height = maxY - this.y;
            };
        })(),

        /**
         * Calculate matrix of transforming from self to target rect
         * @param  {module:zrender/core/BoundingRect} b
         * @return {Array.<number>}
         */
        calculateTransform: function (b) {
            var a = this;
            var sx = b.width / a.width;
            var sy = b.height / a.height;

            var m = matrix.create();

            // 矩阵右乘
            matrix.translate(m, m, [-a.x, -a.y]);
            matrix.scale(m, m, [sx, sy]);
            matrix.translate(m, m, [b.x, b.y]);

            return m;
        },

        /**
         * @param {(module:echarts/core/BoundingRect|Object)} b
         * @return {boolean}
         */
        intersect: function (b) {
            if (!b) {
                return false;
            }

            if (!(b instanceof BoundingRect)) {
                // Normalize negative width/height.
                b = BoundingRect.create(b);
            }

            var a = this;
            var ax0 = a.x;
            var ax1 = a.x + a.width;
            var ay0 = a.y;
            var ay1 = a.y + a.height;

            var bx0 = b.x;
            var bx1 = b.x + b.width;
            var by0 = b.y;
            var by1 = b.y + b.height;

            return ! (ax1 < bx0 || bx1 < ax0 || ay1 < by0 || by1 < ay0);
        },

        contain: function (x, y) {
            var rect = this;
            return x >= rect.x
                && x <= (rect.x + rect.width)
                && y >= rect.y
                && y <= (rect.y + rect.height);
        },

        /**
         * @return {module:echarts/core/BoundingRect}
         */
        clone: function () {
            return new BoundingRect(this.x, this.y, this.width, this.height);
        },

        /**
         * Copy from another rect
         */
        copy: function (other) {
            this.x = other.x;
            this.y = other.y;
            this.width = other.width;
            this.height = other.height;
        },

        plain: function () {
            return {
                x: this.x,
                y: this.y,
                width: this.width,
                height: this.height
            };
        }
    };

    /**
     * @param {Object|module:zrender/core/BoundingRect} rect
     * @param {number} rect.x
     * @param {number} rect.y
     * @param {number} rect.width
     * @param {number} rect.height
     * @return {module:zrender/core/BoundingRect}
     */
    BoundingRect.create = function (rect) {
        return new BoundingRect(rect.x, rect.y, rect.width, rect.height);
    };

    module.exports = BoundingRect;


/***/ }),
/* 4 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * 曲线辅助模块
 * @module zrender/core/curve
 * @author pissang(https://www.github.com/pissang)
 */


    var vec2 = __webpack_require__(2);
    var v2Create = vec2.create;
    var v2DistSquare = vec2.distSquare;
    var mathPow = Math.pow;
    var mathSqrt = Math.sqrt;

    var EPSILON = 1e-8;
    var EPSILON_NUMERIC = 1e-4;

    var THREE_SQRT = mathSqrt(3);
    var ONE_THIRD = 1 / 3;

    // 临时变量
    var _v0 = v2Create();
    var _v1 = v2Create();
    var _v2 = v2Create();
    // var _v3 = vec2.create();

    function isAroundZero(val) {
        return val > -EPSILON && val < EPSILON;
    }
    function isNotAroundZero(val) {
        return val > EPSILON || val < -EPSILON;
    }
    /**
     * 计算三次贝塞尔值
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {number} t
     * @return {number}
     */
    function cubicAt(p0, p1, p2, p3, t) {
        var onet = 1 - t;
        return onet * onet * (onet * p0 + 3 * t * p1)
             + t * t * (t * p3 + 3 * onet * p2);
    }

    /**
     * 计算三次贝塞尔导数值
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {number} t
     * @return {number}
     */
    function cubicDerivativeAt(p0, p1, p2, p3, t) {
        var onet = 1 - t;
        return 3 * (
            ((p1 - p0) * onet + 2 * (p2 - p1) * t) * onet
            + (p3 - p2) * t * t
        );
    }

    /**
     * 计算三次贝塞尔方程根，使用盛金公式
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {number} val
     * @param  {Array.<number>} roots
     * @return {number} 有效根数目
     */
    function cubicRootAt(p0, p1, p2, p3, val, roots) {
        // Evaluate roots of cubic functions
        var a = p3 + 3 * (p1 - p2) - p0;
        var b = 3 * (p2 - p1 * 2 + p0);
        var c = 3 * (p1  - p0);
        var d = p0 - val;

        var A = b * b - 3 * a * c;
        var B = b * c - 9 * a * d;
        var C = c * c - 3 * b * d;

        var n = 0;

        if (isAroundZero(A) && isAroundZero(B)) {
            if (isAroundZero(b)) {
                roots[0] = 0;
            }
            else {
                var t1 = -c / b;  //t1, t2, t3, b is not zero
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
            }
        }
        else {
            var disc = B * B - 4 * A * C;

            if (isAroundZero(disc)) {
                var K = B / A;
                var t1 = -b / a + K;  // t1, a is not zero
                var t2 = -K / 2;  // t2, t3
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
                if (t2 >= 0 && t2 <= 1) {
                    roots[n++] = t2;
                }
            }
            else if (disc > 0) {
                var discSqrt = mathSqrt(disc);
                var Y1 = A * b + 1.5 * a * (-B + discSqrt);
                var Y2 = A * b + 1.5 * a * (-B - discSqrt);
                if (Y1 < 0) {
                    Y1 = -mathPow(-Y1, ONE_THIRD);
                }
                else {
                    Y1 = mathPow(Y1, ONE_THIRD);
                }
                if (Y2 < 0) {
                    Y2 = -mathPow(-Y2, ONE_THIRD);
                }
                else {
                    Y2 = mathPow(Y2, ONE_THIRD);
                }
                var t1 = (-b - (Y1 + Y2)) / (3 * a);
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
            }
            else {
                var T = (2 * A * b - 3 * a * B) / (2 * mathSqrt(A * A * A));
                var theta = Math.acos(T) / 3;
                var ASqrt = mathSqrt(A);
                var tmp = Math.cos(theta);

                var t1 = (-b - 2 * ASqrt * tmp) / (3 * a);
                var t2 = (-b + ASqrt * (tmp + THREE_SQRT * Math.sin(theta))) / (3 * a);
                var t3 = (-b + ASqrt * (tmp - THREE_SQRT * Math.sin(theta))) / (3 * a);
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
                if (t2 >= 0 && t2 <= 1) {
                    roots[n++] = t2;
                }
                if (t3 >= 0 && t3 <= 1) {
                    roots[n++] = t3;
                }
            }
        }
        return n;
    }

    /**
     * 计算三次贝塞尔方程极限值的位置
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {Array.<number>} extrema
     * @return {number} 有效数目
     */
    function cubicExtrema(p0, p1, p2, p3, extrema) {
        var b = 6 * p2 - 12 * p1 + 6 * p0;
        var a = 9 * p1 + 3 * p3 - 3 * p0 - 9 * p2;
        var c = 3 * p1 - 3 * p0;

        var n = 0;
        if (isAroundZero(a)) {
            if (isNotAroundZero(b)) {
                var t1 = -c / b;
                if (t1 >= 0 && t1 <=1) {
                    extrema[n++] = t1;
                }
            }
        }
        else {
            var disc = b * b - 4 * a * c;
            if (isAroundZero(disc)) {
                extrema[0] = -b / (2 * a);
            }
            else if (disc > 0) {
                var discSqrt = mathSqrt(disc);
                var t1 = (-b + discSqrt) / (2 * a);
                var t2 = (-b - discSqrt) / (2 * a);
                if (t1 >= 0 && t1 <= 1) {
                    extrema[n++] = t1;
                }
                if (t2 >= 0 && t2 <= 1) {
                    extrema[n++] = t2;
                }
            }
        }
        return n;
    }

    /**
     * 细分三次贝塞尔曲线
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {number} t
     * @param  {Array.<number>} out
     */
    function cubicSubdivide(p0, p1, p2, p3, t, out) {
        var p01 = (p1 - p0) * t + p0;
        var p12 = (p2 - p1) * t + p1;
        var p23 = (p3 - p2) * t + p2;

        var p012 = (p12 - p01) * t + p01;
        var p123 = (p23 - p12) * t + p12;

        var p0123 = (p123 - p012) * t + p012;
        // Seg0
        out[0] = p0;
        out[1] = p01;
        out[2] = p012;
        out[3] = p0123;
        // Seg1
        out[4] = p0123;
        out[5] = p123;
        out[6] = p23;
        out[7] = p3;
    }

    /**
     * 投射点到三次贝塞尔曲线上，返回投射距离。
     * 投射点有可能会有一个或者多个，这里只返回其中距离最短的一个。
     * @param {number} x0
     * @param {number} y0
     * @param {number} x1
     * @param {number} y1
     * @param {number} x2
     * @param {number} y2
     * @param {number} x3
     * @param {number} y3
     * @param {number} x
     * @param {number} y
     * @param {Array.<number>} [out] 投射点
     * @return {number}
     */
    function cubicProjectPoint(
        x0, y0, x1, y1, x2, y2, x3, y3,
        x, y, out
    ) {
        // http://pomax.github.io/bezierinfo/#projections
        var t;
        var interval = 0.005;
        var d = Infinity;
        var prev;
        var next;
        var d1;
        var d2;

        _v0[0] = x;
        _v0[1] = y;

        // 先粗略估计一下可能的最小距离的 t 值
        // PENDING
        for (var _t = 0; _t < 1; _t += 0.05) {
            _v1[0] = cubicAt(x0, x1, x2, x3, _t);
            _v1[1] = cubicAt(y0, y1, y2, y3, _t);
            d1 = v2DistSquare(_v0, _v1);
            if (d1 < d) {
                t = _t;
                d = d1;
            }
        }
        d = Infinity;

        // At most 32 iteration
        for (var i = 0; i < 32; i++) {
            if (interval < EPSILON_NUMERIC) {
                break;
            }
            prev = t - interval;
            next = t + interval;
            // t - interval
            _v1[0] = cubicAt(x0, x1, x2, x3, prev);
            _v1[1] = cubicAt(y0, y1, y2, y3, prev);

            d1 = v2DistSquare(_v1, _v0);

            if (prev >= 0 && d1 < d) {
                t = prev;
                d = d1;
            }
            else {
                // t + interval
                _v2[0] = cubicAt(x0, x1, x2, x3, next);
                _v2[1] = cubicAt(y0, y1, y2, y3, next);
                d2 = v2DistSquare(_v2, _v0);

                if (next <= 1 && d2 < d) {
                    t = next;
                    d = d2;
                }
                else {
                    interval *= 0.5;
                }
            }
        }
        // t
        if (out) {
            out[0] = cubicAt(x0, x1, x2, x3, t);
            out[1] = cubicAt(y0, y1, y2, y3, t);
        }
        // console.log(interval, i);
        return mathSqrt(d);
    }

    /**
     * 计算二次方贝塞尔值
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} t
     * @return {number}
     */
    function quadraticAt(p0, p1, p2, t) {
        var onet = 1 - t;
        return onet * (onet * p0 + 2 * t * p1) + t * t * p2;
    }

    /**
     * 计算二次方贝塞尔导数值
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} t
     * @return {number}
     */
    function quadraticDerivativeAt(p0, p1, p2, t) {
        return 2 * ((1 - t) * (p1 - p0) + t * (p2 - p1));
    }

    /**
     * 计算二次方贝塞尔方程根
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} t
     * @param  {Array.<number>} roots
     * @return {number} 有效根数目
     */
    function quadraticRootAt(p0, p1, p2, val, roots) {
        var a = p0 - 2 * p1 + p2;
        var b = 2 * (p1 - p0);
        var c = p0 - val;

        var n = 0;
        if (isAroundZero(a)) {
            if (isNotAroundZero(b)) {
                var t1 = -c / b;
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
            }
        }
        else {
            var disc = b * b - 4 * a * c;
            if (isAroundZero(disc)) {
                var t1 = -b / (2 * a);
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
            }
            else if (disc > 0) {
                var discSqrt = mathSqrt(disc);
                var t1 = (-b + discSqrt) / (2 * a);
                var t2 = (-b - discSqrt) / (2 * a);
                if (t1 >= 0 && t1 <= 1) {
                    roots[n++] = t1;
                }
                if (t2 >= 0 && t2 <= 1) {
                    roots[n++] = t2;
                }
            }
        }
        return n;
    }

    /**
     * 计算二次贝塞尔方程极限值
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @return {number}
     */
    function quadraticExtremum(p0, p1, p2) {
        var divider = p0 + p2 - 2 * p1;
        if (divider === 0) {
            // p1 is center of p0 and p2
            return 0.5;
        }
        else {
            return (p0 - p1) / divider;
        }
    }

    /**
     * 细分二次贝塞尔曲线
     * @memberOf module:zrender/core/curve
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} t
     * @param  {Array.<number>} out
     */
    function quadraticSubdivide(p0, p1, p2, t, out) {
        var p01 = (p1 - p0) * t + p0;
        var p12 = (p2 - p1) * t + p1;
        var p012 = (p12 - p01) * t + p01;

        // Seg0
        out[0] = p0;
        out[1] = p01;
        out[2] = p012;

        // Seg1
        out[3] = p012;
        out[4] = p12;
        out[5] = p2;
    }

    /**
     * 投射点到二次贝塞尔曲线上，返回投射距离。
     * 投射点有可能会有一个或者多个，这里只返回其中距离最短的一个。
     * @param {number} x0
     * @param {number} y0
     * @param {number} x1
     * @param {number} y1
     * @param {number} x2
     * @param {number} y2
     * @param {number} x
     * @param {number} y
     * @param {Array.<number>} out 投射点
     * @return {number}
     */
    function quadraticProjectPoint(
        x0, y0, x1, y1, x2, y2,
        x, y, out
    ) {
        // http://pomax.github.io/bezierinfo/#projections
        var t;
        var interval = 0.005;
        var d = Infinity;

        _v0[0] = x;
        _v0[1] = y;

        // 先粗略估计一下可能的最小距离的 t 值
        // PENDING
        for (var _t = 0; _t < 1; _t += 0.05) {
            _v1[0] = quadraticAt(x0, x1, x2, _t);
            _v1[1] = quadraticAt(y0, y1, y2, _t);
            var d1 = v2DistSquare(_v0, _v1);
            if (d1 < d) {
                t = _t;
                d = d1;
            }
        }
        d = Infinity;

        // At most 32 iteration
        for (var i = 0; i < 32; i++) {
            if (interval < EPSILON_NUMERIC) {
                break;
            }
            var prev = t - interval;
            var next = t + interval;
            // t - interval
            _v1[0] = quadraticAt(x0, x1, x2, prev);
            _v1[1] = quadraticAt(y0, y1, y2, prev);

            var d1 = v2DistSquare(_v1, _v0);

            if (prev >= 0 && d1 < d) {
                t = prev;
                d = d1;
            }
            else {
                // t + interval
                _v2[0] = quadraticAt(x0, x1, x2, next);
                _v2[1] = quadraticAt(y0, y1, y2, next);
                var d2 = v2DistSquare(_v2, _v0);
                if (next <= 1 && d2 < d) {
                    t = next;
                    d = d2;
                }
                else {
                    interval *= 0.5;
                }
            }
        }
        // t
        if (out) {
            out[0] = quadraticAt(x0, x1, x2, t);
            out[1] = quadraticAt(y0, y1, y2, t);
        }
        // console.log(interval, i);
        return mathSqrt(d);
    }

    module.exports = {

        cubicAt: cubicAt,

        cubicDerivativeAt: cubicDerivativeAt,

        cubicRootAt: cubicRootAt,

        cubicExtrema: cubicExtrema,

        cubicSubdivide: cubicSubdivide,

        cubicProjectPoint: cubicProjectPoint,

        quadraticAt: quadraticAt,

        quadraticDerivativeAt: quadraticDerivativeAt,

        quadraticRootAt: quadraticRootAt,

        quadraticExtremum: quadraticExtremum,

        quadraticSubdivide: quadraticSubdivide,

        quadraticProjectPoint: quadraticProjectPoint
    };


/***/ }),
/* 5 */
/***/ (function(module, exports, __webpack_require__) {



    var textWidthCache = {};
    var textWidthCacheCounter = 0;
    var TEXT_CACHE_MAX = 5000;

    var util = __webpack_require__(0);
    var BoundingRect = __webpack_require__(3);
    var retrieve = util.retrieve;

    function getTextWidth(text, textFont) {
        var key = text + ':' + textFont;
        if (textWidthCache[key]) {
            return textWidthCache[key];
        }

        var textLines = (text + '').split('\n');
        var width = 0;

        for (var i = 0, l = textLines.length; i < l; i++) {
            // measureText 可以被覆盖以兼容不支持 Canvas 的环境
            width = Math.max(textContain.measureText(textLines[i], textFont).width, width);
        }

        if (textWidthCacheCounter > TEXT_CACHE_MAX) {
            textWidthCacheCounter = 0;
            textWidthCache = {};
        }
        textWidthCacheCounter++;
        textWidthCache[key] = width;

        return width;
    }

    function getTextRect(text, textFont, textAlign, textBaseline) {
        var textLineLen = ((text || '') + '').split('\n').length;

        var width = getTextWidth(text, textFont);
        // FIXME 高度计算比较粗暴
        var lineHeight = getTextWidth('国', textFont);
        var height = textLineLen * lineHeight;

        var rect = new BoundingRect(0, 0, width, height);
        // Text has a special line height property
        rect.lineHeight = lineHeight;

        switch (textBaseline) {
            case 'bottom':
            case 'alphabetic':
                rect.y -= lineHeight;
                break;
            case 'middle':
                rect.y -= lineHeight / 2;
                break;
            // case 'hanging':
            // case 'top':
        }

        // FIXME Right to left language
        switch (textAlign) {
            case 'end':
            case 'right':
                rect.x -= rect.width;
                break;
            case 'center':
                rect.x -= rect.width / 2;
                break;
            // case 'start':
            // case 'left':
        }

        return rect;
    }

    function adjustTextPositionOnRect(textPosition, rect, textRect, distance) {

        var x = rect.x;
        var y = rect.y;

        var height = rect.height;
        var width = rect.width;

        var textHeight = textRect.height;

        var lineHeight = textRect.lineHeight;
        var halfHeight = height / 2 - textHeight / 2 + lineHeight;

        var textAlign = 'left';

        switch (textPosition) {
            case 'left':
                x -= distance;
                y += halfHeight;
                textAlign = 'right';
                break;
            case 'right':
                x += distance + width;
                y += halfHeight;
                textAlign = 'left';
                break;
            case 'top':
                x += width / 2;
                y -= distance + textHeight - lineHeight;
                textAlign = 'center';
                break;
            case 'bottom':
                x += width / 2;
                y += height + distance + lineHeight;
                textAlign = 'center';
                break;
            case 'inside':
                x += width / 2;
                y += halfHeight;
                textAlign = 'center';
                break;
            case 'insideLeft':
                x += distance;
                y += halfHeight;
                textAlign = 'left';
                break;
            case 'insideRight':
                x += width - distance;
                y += halfHeight;
                textAlign = 'right';
                break;
            case 'insideTop':
                x += width / 2;
                y += distance + lineHeight;
                textAlign = 'center';
                break;
            case 'insideBottom':
                x += width / 2;
                y += height - textHeight - distance + lineHeight;
                textAlign = 'center';
                break;
            case 'insideTopLeft':
                x += distance;
                y += distance + lineHeight;
                textAlign = 'left';
                break;
            case 'insideTopRight':
                x += width - distance;
                y += distance + lineHeight;
                textAlign = 'right';
                break;
            case 'insideBottomLeft':
                x += distance;
                y += height - textHeight - distance + lineHeight;
                break;
            case 'insideBottomRight':
                x += width - distance;
                y += height - textHeight - distance + lineHeight;
                textAlign = 'right';
                break;
        }

        return {
            x: x,
            y: y,
            textAlign: textAlign,
            textBaseline: 'alphabetic'
        };
    }

    /**
     * Show ellipsis if overflow.
     *
     * @param  {string} text
     * @param  {string} containerWidth
     * @param  {string} textFont
     * @param  {number} [ellipsis='...']
     * @param  {Object} [options]
     * @param  {number} [options.maxIterations=3]
     * @param  {number} [options.minChar=0] If truncate result are less
     *                  then minChar, ellipsis will not show, which is
     *                  better for user hint in some cases.
     * @param  {number} [options.placeholder=''] When all truncated, use the placeholder.
     * @return {string}
     */
    function truncateText(text, containerWidth, textFont, ellipsis, options) {
        if (!containerWidth) {
            return '';
        }

        options = options || {};

        ellipsis = retrieve(ellipsis, '...');
        var maxIterations = retrieve(options.maxIterations, 2);
        var minChar = retrieve(options.minChar, 0);
        // FIXME
        // Other languages?
        var cnCharWidth = getTextWidth('国', textFont);
        // FIXME
        // Consider proportional font?
        var ascCharWidth = getTextWidth('a', textFont);
        var placeholder = retrieve(options.placeholder, '');

        // Example 1: minChar: 3, text: 'asdfzxcv', truncate result: 'asdf', but not: 'a...'.
        // Example 2: minChar: 3, text: '维度', truncate result: '维', but not: '...'.
        var contentWidth = containerWidth = Math.max(0, containerWidth - 1); // Reserve some gap.
        for (var i = 0; i < minChar && contentWidth >= ascCharWidth; i++) {
            contentWidth -= ascCharWidth;
        }

        var ellipsisWidth = getTextWidth(ellipsis);
        if (ellipsisWidth > contentWidth) {
            ellipsis = '';
            ellipsisWidth = 0;
        }

        contentWidth = containerWidth - ellipsisWidth;

        var textLines = (text + '').split('\n');

        for (var i = 0, len = textLines.length; i < len; i++) {
            var textLine = textLines[i];
            var lineWidth = getTextWidth(textLine, textFont);

            if (lineWidth <= containerWidth) {
                continue;
            }

            for (var j = 0;; j++) {
                if (lineWidth <= contentWidth || j >= maxIterations) {
                    textLine += ellipsis;
                    break;
                }

                var subLength = j === 0
                    ? estimateLength(textLine, contentWidth, ascCharWidth, cnCharWidth)
                    : lineWidth > 0
                    ? Math.floor(textLine.length * contentWidth / lineWidth)
                    : 0;

                textLine = textLine.substr(0, subLength);
                lineWidth = getTextWidth(textLine, textFont);
            }

            if (textLine === '') {
                textLine = placeholder;
            }

            textLines[i] = textLine;
        }

        return textLines.join('\n');
    }

    function estimateLength(text, contentWidth, ascCharWidth, cnCharWidth) {
        var width = 0;
        var i = 0;
        for (var len = text.length; i < len && width < contentWidth; i++) {
            var charCode = text.charCodeAt(i);
            width += (0 <= charCode && charCode <= 127) ? ascCharWidth : cnCharWidth;
        }
        return i;
    }

    var textContain = {

        getWidth: getTextWidth,

        getBoundingRect: getTextRect,

        adjustTextPositionOnRect: adjustTextPositionOnRect,

        truncateText: truncateText,

        measureText: function (text, textFont) {
            var ctx = util.getContext();
            ctx.font = textFont || '12px sans-serif';
            return ctx.measureText(text);
        }
    };

    module.exports = textContain;


/***/ }),
/* 6 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * Path 代理，可以在`buildPath`中用于替代`ctx`, 会保存每个path操作的命令到pathCommands属性中
 * 可以用于 isInsidePath 判断以及获取boundingRect
 *
 * @module zrender/core/PathProxy
 * @author Yi Shen (http://www.github.com/pissang)
 */

 // TODO getTotalLength, getPointAtLength


    var curve = __webpack_require__(4);
    var vec2 = __webpack_require__(2);
    var bbox = __webpack_require__(44);
    var BoundingRect = __webpack_require__(3);
    var dpr = __webpack_require__(18).devicePixelRatio;

    var CMD = {
        M: 1,
        L: 2,
        C: 3,
        Q: 4,
        A: 5,
        Z: 6,
        // Rect
        R: 7
    };

    // var CMD_MEM_SIZE = {
    //     M: 3,
    //     L: 3,
    //     C: 7,
    //     Q: 5,
    //     A: 9,
    //     R: 5,
    //     Z: 1
    // };

    var min = [];
    var max = [];
    var min2 = [];
    var max2 = [];
    var mathMin = Math.min;
    var mathMax = Math.max;
    var mathCos = Math.cos;
    var mathSin = Math.sin;
    var mathSqrt = Math.sqrt;
    var mathAbs = Math.abs;

    var hasTypedArray = typeof Float32Array != 'undefined';

    /**
     * @alias module:zrender/core/PathProxy
     * @constructor
     */
    var PathProxy = function (notSaveData) {

        this._saveData = !(notSaveData || false);

        if (this._saveData) {
            /**
             * Path data. Stored as flat array
             * @type {Array.<Object>}
             */
            this.data = [];
        }

        this._ctx = null;
    };

    /**
     * 快速计算Path包围盒（并不是最小包围盒）
     * @return {Object}
     */
    PathProxy.prototype = {

        constructor: PathProxy,

        _xi: 0,
        _yi: 0,

        _x0: 0,
        _y0: 0,
        // Unit x, Unit y. Provide for avoiding drawing that too short line segment
        _ux: 0,
        _uy: 0,

        _len: 0,

        _lineDash: null,

        _dashOffset: 0,

        _dashIdx: 0,

        _dashSum: 0,

        /**
         * @readOnly
         */
        setScale: function (sx, sy) {
            this._ux = mathAbs(1 / dpr / sx) || 0;
            this._uy = mathAbs(1 / dpr / sy) || 0;
        },

        getContext: function () {
            return this._ctx;
        },

        /**
         * @param  {CanvasRenderingContext2D} ctx
         * @return {module:zrender/core/PathProxy}
         */
        beginPath: function (ctx) {

            this._ctx = ctx;

            ctx && ctx.beginPath();

            ctx && (this.dpr = ctx.dpr);

            // Reset
            if (this._saveData) {
                this._len = 0;
            }

            if (this._lineDash) {
                this._lineDash = null;

                this._dashOffset = 0;
            }

            return this;
        },

        /**
         * @param  {number} x
         * @param  {number} y
         * @return {module:zrender/core/PathProxy}
         */
        moveTo: function (x, y) {
            this.addData(CMD.M, x, y);
            this._ctx && this._ctx.moveTo(x, y);

            // x0, y0, xi, yi 是记录在 _dashedXXXXTo 方法中使用
            // xi, yi 记录当前点, x0, y0 在 closePath 的时候回到起始点。
            // 有可能在 beginPath 之后直接调用 lineTo，这时候 x0, y0 需要
            // 在 lineTo 方法中记录，这里先不考虑这种情况，dashed line 也只在 IE10- 中不支持
            this._x0 = x;
            this._y0 = y;

            this._xi = x;
            this._yi = y;

            return this;
        },

        /**
         * @param  {number} x
         * @param  {number} y
         * @return {module:zrender/core/PathProxy}
         */
        lineTo: function (x, y) {
            var exceedUnit = mathAbs(x - this._xi) > this._ux
                || mathAbs(y - this._yi) > this._uy
                // Force draw the first segment
                || this._len < 5;

            this.addData(CMD.L, x, y);

            if (this._ctx && exceedUnit) {
                this._needsDash() ? this._dashedLineTo(x, y)
                    : this._ctx.lineTo(x, y);
            }
            if (exceedUnit) {
                this._xi = x;
                this._yi = y;
            }

            return this;
        },

        /**
         * @param  {number} x1
         * @param  {number} y1
         * @param  {number} x2
         * @param  {number} y2
         * @param  {number} x3
         * @param  {number} y3
         * @return {module:zrender/core/PathProxy}
         */
        bezierCurveTo: function (x1, y1, x2, y2, x3, y3) {
            this.addData(CMD.C, x1, y1, x2, y2, x3, y3);
            if (this._ctx) {
                this._needsDash() ? this._dashedBezierTo(x1, y1, x2, y2, x3, y3)
                    : this._ctx.bezierCurveTo(x1, y1, x2, y2, x3, y3);
            }
            this._xi = x3;
            this._yi = y3;
            return this;
        },

        /**
         * @param  {number} x1
         * @param  {number} y1
         * @param  {number} x2
         * @param  {number} y2
         * @return {module:zrender/core/PathProxy}
         */
        quadraticCurveTo: function (x1, y1, x2, y2) {
            this.addData(CMD.Q, x1, y1, x2, y2);
            if (this._ctx) {
                this._needsDash() ? this._dashedQuadraticTo(x1, y1, x2, y2)
                    : this._ctx.quadraticCurveTo(x1, y1, x2, y2);
            }
            this._xi = x2;
            this._yi = y2;
            return this;
        },

        /**
         * @param  {number} cx
         * @param  {number} cy
         * @param  {number} r
         * @param  {number} startAngle
         * @param  {number} endAngle
         * @param  {boolean} anticlockwise
         * @return {module:zrender/core/PathProxy}
         */
        arc: function (cx, cy, r, startAngle, endAngle, anticlockwise) {
            this.addData(
                CMD.A, cx, cy, r, r, startAngle, endAngle - startAngle, 0, anticlockwise ? 0 : 1
            );
            this._ctx && this._ctx.arc(cx, cy, r, startAngle, endAngle, anticlockwise);

            this._xi = mathCos(endAngle) * r + cx;
            this._yi = mathSin(endAngle) * r + cx;
            return this;
        },

        // TODO
        arcTo: function (x1, y1, x2, y2, radius) {
            if (this._ctx) {
                this._ctx.arcTo(x1, y1, x2, y2, radius);
            }
            return this;
        },

        // TODO
        rect: function (x, y, w, h) {
            this._ctx && this._ctx.rect(x, y, w, h);
            this.addData(CMD.R, x, y, w, h);
            return this;
        },

        /**
         * @return {module:zrender/core/PathProxy}
         */
        closePath: function () {
            this.addData(CMD.Z);

            var ctx = this._ctx;
            var x0 = this._x0;
            var y0 = this._y0;
            if (ctx) {
                this._needsDash() && this._dashedLineTo(x0, y0);
                ctx.closePath();
            }

            this._xi = x0;
            this._yi = y0;
            return this;
        },

        /**
         * Context 从外部传入，因为有可能是 rebuildPath 完之后再 fill。
         * stroke 同样
         * @param {CanvasRenderingContext2D} ctx
         * @return {module:zrender/core/PathProxy}
         */
        fill: function (ctx) {
            ctx && ctx.fill();
            this.toStatic();
        },

        /**
         * @param {CanvasRenderingContext2D} ctx
         * @return {module:zrender/core/PathProxy}
         */
        stroke: function (ctx) {
            ctx && ctx.stroke();
            this.toStatic();
        },

        /**
         * 必须在其它绘制命令前调用
         * Must be invoked before all other path drawing methods
         * @return {module:zrender/core/PathProxy}
         */
        setLineDash: function (lineDash) {
            if (lineDash instanceof Array) {
                this._lineDash = lineDash;

                this._dashIdx = 0;

                var lineDashSum = 0;
                for (var i = 0; i < lineDash.length; i++) {
                    lineDashSum += lineDash[i];
                }
                this._dashSum = lineDashSum;
            }
            return this;
        },

        /**
         * 必须在其它绘制命令前调用
         * Must be invoked before all other path drawing methods
         * @return {module:zrender/core/PathProxy}
         */
        setLineDashOffset: function (offset) {
            this._dashOffset = offset;
            return this;
        },

        /**
         *
         * @return {boolean}
         */
        len: function () {
            return this._len;
        },

        /**
         * 直接设置 Path 数据
         */
        setData: function (data) {

            var len = data.length;

            if (! (this.data && this.data.length == len) && hasTypedArray) {
                this.data = new Float32Array(len);
            }

            for (var i = 0; i < len; i++) {
                this.data[i] = data[i];
            }

            this._len = len;
        },

        /**
         * 添加子路径
         * @param {module:zrender/core/PathProxy|Array.<module:zrender/core/PathProxy>} path
         */
        appendPath: function (path) {
            if (!(path instanceof Array)) {
                path = [path];
            }
            var len = path.length;
            var appendSize = 0;
            var offset = this._len;
            for (var i = 0; i < len; i++) {
                appendSize += path[i].len();
            }
            if (hasTypedArray && (this.data instanceof Float32Array)) {
                this.data = new Float32Array(offset + appendSize);
            }
            for (var i = 0; i < len; i++) {
                var appendPathData = path[i].data;
                for (var k = 0; k < appendPathData.length; k++) {
                    this.data[offset++] = appendPathData[k];
                }
            }
            this._len = offset;
        },

        /**
         * 填充 Path 数据。
         * 尽量复用而不申明新的数组。大部分图形重绘的指令数据长度都是不变的。
         */
        addData: function (cmd) {
            if (!this._saveData) {
                return;
            }

            var data = this.data;
            if (this._len + arguments.length > data.length) {
                // 因为之前的数组已经转换成静态的 Float32Array
                // 所以不够用时需要扩展一个新的动态数组
                this._expandData();
                data = this.data;
            }
            for (var i = 0; i < arguments.length; i++) {
                data[this._len++] = arguments[i];
            }

            this._prevCmd = cmd;
        },

        _expandData: function () {
            // Only if data is Float32Array
            if (!(this.data instanceof Array)) {
                var newData = [];
                for (var i = 0; i < this._len; i++) {
                    newData[i] = this.data[i];
                }
                this.data = newData;
            }
        },

        /**
         * If needs js implemented dashed line
         * @return {boolean}
         * @private
         */
        _needsDash: function () {
            return this._lineDash;
        },

        _dashedLineTo: function (x1, y1) {
            var dashSum = this._dashSum;
            var offset = this._dashOffset;
            var lineDash = this._lineDash;
            var ctx = this._ctx;

            var x0 = this._xi;
            var y0 = this._yi;
            var dx = x1 - x0;
            var dy = y1 - y0;
            var dist = mathSqrt(dx * dx + dy * dy);
            var x = x0;
            var y = y0;
            var dash;
            var nDash = lineDash.length;
            var idx;
            dx /= dist;
            dy /= dist;

            if (offset < 0) {
                // Convert to positive offset
                offset = dashSum + offset;
            }
            offset %= dashSum;
            x -= offset * dx;
            y -= offset * dy;

            while ((dx > 0 && x <= x1) || (dx < 0 && x >= x1)
            || (dx == 0 && ((dy > 0 && y <= y1) || (dy < 0 && y >= y1)))) {
                idx = this._dashIdx;
                dash = lineDash[idx];
                x += dx * dash;
                y += dy * dash;
                this._dashIdx = (idx + 1) % nDash;
                // Skip positive offset
                if ((dx > 0 && x < x0) || (dx < 0 && x > x0) || (dy > 0 && y < y0) || (dy < 0 && y > y0)) {
                    continue;
                }
                ctx[idx % 2 ? 'moveTo' : 'lineTo'](
                    dx >= 0 ? mathMin(x, x1) : mathMax(x, x1),
                    dy >= 0 ? mathMin(y, y1) : mathMax(y, y1)
                );
            }
            // Offset for next lineTo
            dx = x - x1;
            dy = y - y1;
            this._dashOffset = -mathSqrt(dx * dx + dy * dy);
        },

        // Not accurate dashed line to
        _dashedBezierTo: function (x1, y1, x2, y2, x3, y3) {
            var dashSum = this._dashSum;
            var offset = this._dashOffset;
            var lineDash = this._lineDash;
            var ctx = this._ctx;

            var x0 = this._xi;
            var y0 = this._yi;
            var t;
            var dx;
            var dy;
            var cubicAt = curve.cubicAt;
            var bezierLen = 0;
            var idx = this._dashIdx;
            var nDash = lineDash.length;

            var x;
            var y;

            var tmpLen = 0;

            if (offset < 0) {
                // Convert to positive offset
                offset = dashSum + offset;
            }
            offset %= dashSum;
            // Bezier approx length
            for (t = 0; t < 1; t += 0.1) {
                dx = cubicAt(x0, x1, x2, x3, t + 0.1)
                    - cubicAt(x0, x1, x2, x3, t);
                dy = cubicAt(y0, y1, y2, y3, t + 0.1)
                    - cubicAt(y0, y1, y2, y3, t);
                bezierLen += mathSqrt(dx * dx + dy * dy);
            }

            // Find idx after add offset
            for (; idx < nDash; idx++) {
                tmpLen += lineDash[idx];
                if (tmpLen > offset) {
                    break;
                }
            }
            t = (tmpLen - offset) / bezierLen;

            while (t <= 1) {

                x = cubicAt(x0, x1, x2, x3, t);
                y = cubicAt(y0, y1, y2, y3, t);

                // Use line to approximate dashed bezier
                // Bad result if dash is long
                idx % 2 ? ctx.moveTo(x, y)
                    : ctx.lineTo(x, y);

                t += lineDash[idx] / bezierLen;

                idx = (idx + 1) % nDash;
            }

            // Finish the last segment and calculate the new offset
            (idx % 2 !== 0) && ctx.lineTo(x3, y3);
            dx = x3 - x;
            dy = y3 - y;
            this._dashOffset = -mathSqrt(dx * dx + dy * dy);
        },

        _dashedQuadraticTo: function (x1, y1, x2, y2) {
            // Convert quadratic to cubic using degree elevation
            var x3 = x2;
            var y3 = y2;
            x2 = (x2 + 2 * x1) / 3;
            y2 = (y2 + 2 * y1) / 3;
            x1 = (this._xi + 2 * x1) / 3;
            y1 = (this._yi + 2 * y1) / 3;

            this._dashedBezierTo(x1, y1, x2, y2, x3, y3);
        },

        /**
         * 转成静态的 Float32Array 减少堆内存占用
         * Convert dynamic array to static Float32Array
         */
        toStatic: function () {
            var data = this.data;
            if (data instanceof Array) {
                data.length = this._len;
                if (hasTypedArray) {
                    this.data = new Float32Array(data);
                }
            }
        },

        /**
         * @return {module:zrender/core/BoundingRect}
         */
        getBoundingRect: function () {
            min[0] = min[1] = min2[0] = min2[1] = Number.MAX_VALUE;
            max[0] = max[1] = max2[0] = max2[1] = -Number.MAX_VALUE;

            var data = this.data;
            var xi = 0;
            var yi = 0;
            var x0 = 0;
            var y0 = 0;

            for (var i = 0; i < data.length;) {
                var cmd = data[i++];

                if (i == 1) {
                    // 如果第一个命令是 L, C, Q
                    // 则 previous point 同绘制命令的第一个 point
                    //
                    // 第一个命令为 Arc 的情况下会在后面特殊处理
                    xi = data[i];
                    yi = data[i + 1];

                    x0 = xi;
                    y0 = yi;
                }

                switch (cmd) {
                    case CMD.M:
                        // moveTo 命令重新创建一个新的 subpath, 并且更新新的起点
                        // 在 closePath 的时候使用
                        x0 = data[i++];
                        y0 = data[i++];
                        xi = x0;
                        yi = y0;
                        min2[0] = x0;
                        min2[1] = y0;
                        max2[0] = x0;
                        max2[1] = y0;
                        break;
                    case CMD.L:
                        bbox.fromLine(xi, yi, data[i], data[i + 1], min2, max2);
                        xi = data[i++];
                        yi = data[i++];
                        break;
                    case CMD.C:
                        bbox.fromCubic(
                            xi, yi, data[i++], data[i++], data[i++], data[i++], data[i], data[i + 1],
                            min2, max2
                        );
                        xi = data[i++];
                        yi = data[i++];
                        break;
                    case CMD.Q:
                        bbox.fromQuadratic(
                            xi, yi, data[i++], data[i++], data[i], data[i + 1],
                            min2, max2
                        );
                        xi = data[i++];
                        yi = data[i++];
                        break;
                    case CMD.A:
                        // TODO Arc 判断的开销比较大
                        var cx = data[i++];
                        var cy = data[i++];
                        var rx = data[i++];
                        var ry = data[i++];
                        var startAngle = data[i++];
                        var endAngle = data[i++] + startAngle;
                        // TODO Arc 旋转
                        var psi = data[i++];
                        var anticlockwise = 1 - data[i++];

                        if (i == 1) {
                            // 直接使用 arc 命令
                            // 第一个命令起点还未定义
                            x0 = mathCos(startAngle) * rx + cx;
                            y0 = mathSin(startAngle) * ry + cy;
                        }

                        bbox.fromArc(
                            cx, cy, rx, ry, startAngle, endAngle,
                            anticlockwise, min2, max2
                        );

                        xi = mathCos(endAngle) * rx + cx;
                        yi = mathSin(endAngle) * ry + cy;
                        break;
                    case CMD.R:
                        x0 = xi = data[i++];
                        y0 = yi = data[i++];
                        var width = data[i++];
                        var height = data[i++];
                        // Use fromLine
                        bbox.fromLine(x0, y0, x0 + width, y0 + height, min2, max2);
                        break;
                    case CMD.Z:
                        xi = x0;
                        yi = y0;
                        break;
                }

                // Union
                vec2.min(min, min, min2);
                vec2.max(max, max, max2);
            }

            // No data
            if (i === 0) {
                min[0] = min[1] = max[0] = max[1] = 0;
            }

            return new BoundingRect(
                min[0], min[1], max[0] - min[0], max[1] - min[1]
            );
        },

        /**
         * Rebuild path from current data
         * Rebuild path will not consider javascript implemented line dash.
         * @param {CanvasRenderingContext} ctx
         */
        rebuildPath: function (ctx) {
            var d = this.data;
            var x0, y0;
            var xi, yi;
            var x, y;
            var ux = this._ux;
            var uy = this._uy;
            var len = this._len;
            for (var i = 0; i < len;) {
                var cmd = d[i++];

                if (i == 1) {
                    // 如果第一个命令是 L, C, Q
                    // 则 previous point 同绘制命令的第一个 point
                    //
                    // 第一个命令为 Arc 的情况下会在后面特殊处理
                    xi = d[i];
                    yi = d[i + 1];

                    x0 = xi;
                    y0 = yi;
                }
                switch (cmd) {
                    case CMD.M:
                        x0 = xi = d[i++];
                        y0 = yi = d[i++];
                        ctx.moveTo(xi, yi);
                        break;
                    case CMD.L:
                        x = d[i++];
                        y = d[i++];
                        // Not draw too small seg between
                        if (mathAbs(x - xi) > ux || mathAbs(y - yi) > uy || i === len - 1) {
                            ctx.lineTo(x, y);
                            xi = x;
                            yi = y;
                        }
                        break;
                    case CMD.C:
                        ctx.bezierCurveTo(
                            d[i++], d[i++], d[i++], d[i++], d[i++], d[i++]
                        );
                        xi = d[i - 2];
                        yi = d[i - 1];
                        break;
                    case CMD.Q:
                        ctx.quadraticCurveTo(d[i++], d[i++], d[i++], d[i++]);
                        xi = d[i - 2];
                        yi = d[i - 1];
                        break;
                    case CMD.A:
                        var cx = d[i++];
                        var cy = d[i++];
                        var rx = d[i++];
                        var ry = d[i++];
                        var theta = d[i++];
                        var dTheta = d[i++];
                        var psi = d[i++];
                        var fs = d[i++];
                        var r = (rx > ry) ? rx : ry;
                        var scaleX = (rx > ry) ? 1 : rx / ry;
                        var scaleY = (rx > ry) ? ry / rx : 1;
                        var isEllipse = Math.abs(rx - ry) > 1e-3;
                        var endAngle = theta + dTheta;
                        if (isEllipse) {
                            ctx.translate(cx, cy);
                            ctx.rotate(psi);
                            ctx.scale(scaleX, scaleY);
                            ctx.arc(0, 0, r, theta, endAngle, 1 - fs);
                            ctx.scale(1 / scaleX, 1 / scaleY);
                            ctx.rotate(-psi);
                            ctx.translate(-cx, -cy);
                        }
                        else {
                            ctx.arc(cx, cy, r, theta, endAngle, 1 - fs);
                        }

                        if (i == 1) {
                            // 直接使用 arc 命令
                            // 第一个命令起点还未定义
                            x0 = mathCos(theta) * rx + cx;
                            y0 = mathSin(theta) * ry + cy;
                        }
                        xi = mathCos(endAngle) * rx + cx;
                        yi = mathSin(endAngle) * ry + cy;
                        break;
                    case CMD.R:
                        x0 = xi = d[i];
                        y0 = yi = d[i + 1];
                        ctx.rect(d[i++], d[i++], d[i++], d[i++]);
                        break;
                    case CMD.Z:
                        ctx.closePath();
                        xi = x0;
                        yi = y0;
                }
            }
        }
    };

    PathProxy.CMD = CMD;

    module.exports = PathProxy;


/***/ }),
/* 7 */
/***/ (function(module, exports) {

module.exports = __WEBPACK_EXTERNAL_MODULE_7__;

/***/ }),
/* 8 */
/***/ (function(module, exports) {


    var ArrayCtor = typeof Float32Array === 'undefined'
        ? Array
        : Float32Array;
    /**
     * 3x2矩阵操作类
     * @exports zrender/tool/matrix
     */
    var matrix = {
        /**
         * 创建一个单位矩阵
         * @return {Float32Array|Array.<number>}
         */
        create : function() {
            var out = new ArrayCtor(6);
            matrix.identity(out);

            return out;
        },
        /**
         * 设置矩阵为单位矩阵
         * @param {Float32Array|Array.<number>} out
         */
        identity : function(out) {
            out[0] = 1;
            out[1] = 0;
            out[2] = 0;
            out[3] = 1;
            out[4] = 0;
            out[5] = 0;
            return out;
        },
        /**
         * 复制矩阵
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} m
         */
        copy: function(out, m) {
            out[0] = m[0];
            out[1] = m[1];
            out[2] = m[2];
            out[3] = m[3];
            out[4] = m[4];
            out[5] = m[5];
            return out;
        },
        /**
         * 矩阵相乘
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} m1
         * @param {Float32Array|Array.<number>} m2
         */
        mul : function (out, m1, m2) {
            // Consider matrix.mul(m, m2, m);
            // where out is the same as m2.
            // So use temp variable to escape error.
            var out0 = m1[0] * m2[0] + m1[2] * m2[1];
            var out1 = m1[1] * m2[0] + m1[3] * m2[1];
            var out2 = m1[0] * m2[2] + m1[2] * m2[3];
            var out3 = m1[1] * m2[2] + m1[3] * m2[3];
            var out4 = m1[0] * m2[4] + m1[2] * m2[5] + m1[4];
            var out5 = m1[1] * m2[4] + m1[3] * m2[5] + m1[5];
            out[0] = out0;
            out[1] = out1;
            out[2] = out2;
            out[3] = out3;
            out[4] = out4;
            out[5] = out5;
            return out;
        },
        /**
         * 平移变换
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} a
         * @param {Float32Array|Array.<number>} v
         */
        translate : function(out, a, v) {
            out[0] = a[0];
            out[1] = a[1];
            out[2] = a[2];
            out[3] = a[3];
            out[4] = a[4] + v[0];
            out[5] = a[5] + v[1];
            return out;
        },
        /**
         * 旋转变换
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} a
         * @param {number} rad
         */
        rotate : function(out, a, rad) {
            var aa = a[0];
            var ac = a[2];
            var atx = a[4];
            var ab = a[1];
            var ad = a[3];
            var aty = a[5];
            var st = Math.sin(rad);
            var ct = Math.cos(rad);

            out[0] = aa * ct + ab * st;
            out[1] = -aa * st + ab * ct;
            out[2] = ac * ct + ad * st;
            out[3] = -ac * st + ct * ad;
            out[4] = ct * atx + st * aty;
            out[5] = ct * aty - st * atx;
            return out;
        },
        /**
         * 缩放变换
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} a
         * @param {Float32Array|Array.<number>} v
         */
        scale : function(out, a, v) {
            var vx = v[0];
            var vy = v[1];
            out[0] = a[0] * vx;
            out[1] = a[1] * vy;
            out[2] = a[2] * vx;
            out[3] = a[3] * vy;
            out[4] = a[4] * vx;
            out[5] = a[5] * vy;
            return out;
        },
        /**
         * 求逆矩阵
         * @param {Float32Array|Array.<number>} out
         * @param {Float32Array|Array.<number>} a
         */
        invert : function(out, a) {

            var aa = a[0];
            var ac = a[2];
            var atx = a[4];
            var ab = a[1];
            var ad = a[3];
            var aty = a[5];

            var det = aa * ad - ab * ac;
            if (!det) {
                return null;
            }
            det = 1.0 / det;

            out[0] = ad * det;
            out[1] = -ab * det;
            out[2] = -ac * det;
            out[3] = aa * det;
            out[4] = (ac * aty - ad * atx) * det;
            out[5] = (ab * atx - aa * aty) * det;
            return out;
        }
    };

    module.exports = matrix;



/***/ }),
/* 9 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 数值处理模块
 * @module echarts/util/number
 */



    var zrUtil = __webpack_require__(0);

    var number = {};

    var RADIAN_EPSILON = 1e-4;

    function _trim(str) {
        return str.replace(/^\s+/, '').replace(/\s+$/, '');
    }

    /**
     * Linear mapping a value from domain to range
     * @memberOf module:echarts/util/number
     * @param  {(number|Array.<number>)} val
     * @param  {Array.<number>} domain Domain extent domain[0] can be bigger than domain[1]
     * @param  {Array.<number>} range  Range extent range[0] can be bigger than range[1]
     * @param  {boolean} clamp
     * @return {(number|Array.<number>}
     */
    number.linearMap = function (val, domain, range, clamp) {
        var subDomain = domain[1] - domain[0];
        var subRange = range[1] - range[0];

        if (subDomain === 0) {
            return subRange === 0
                ? range[0]
                : (range[0] + range[1]) / 2;
        }

        // Avoid accuracy problem in edge, such as
        // 146.39 - 62.83 === 83.55999999999999.
        // See echarts/test/ut/spec/util/number.js#linearMap#accuracyError
        // It is a little verbose for efficiency considering this method
        // is a hotspot.
        if (clamp) {
            if (subDomain > 0) {
                if (val <= domain[0]) {
                    return range[0];
                }
                else if (val >= domain[1]) {
                    return range[1];
                }
            }
            else {
                if (val >= domain[0]) {
                    return range[0];
                }
                else if (val <= domain[1]) {
                    return range[1];
                }
            }
        }
        else {
            if (val === domain[0]) {
                return range[0];
            }
            if (val === domain[1]) {
                return range[1];
            }
        }

        return (val - domain[0]) / subDomain * subRange + range[0];
    };

    /**
     * Convert a percent string to absolute number.
     * Returns NaN if percent is not a valid string or number
     * @memberOf module:echarts/util/number
     * @param {string|number} percent
     * @param {number} all
     * @return {number}
     */
    number.parsePercent = function(percent, all) {
        switch (percent) {
            case 'center':
            case 'middle':
                percent = '50%';
                break;
            case 'left':
            case 'top':
                percent = '0%';
                break;
            case 'right':
            case 'bottom':
                percent = '100%';
                break;
        }
        if (typeof percent === 'string') {
            if (_trim(percent).match(/%$/)) {
                return parseFloat(percent) / 100 * all;
            }

            return parseFloat(percent);
        }

        return percent == null ? NaN : +percent;
    };

    /**
     * (1) Fix rounding error of float numbers.
     * (2) Support return string to avoid scientific notation like '3.5e-7'.
     *
     * @param {number} x
     * @param {number} [precision]
     * @param {boolean} [returnStr]
     * @return {number|string}
     */
    number.round = function (x, precision, returnStr) {
        if (precision == null) {
            precision = 10;
        }
        // Avoid range error
        precision = Math.min(Math.max(0, precision), 20);
        x = (+x).toFixed(precision);
        return returnStr ? x : +x;
    };

    number.asc = function (arr) {
        arr.sort(function (a, b) {
            return a - b;
        });
        return arr;
    };

    /**
     * Get precision
     * @param {number} val
     */
    number.getPrecision = function (val) {
        val = +val;
        if (isNaN(val)) {
            return 0;
        }
        // It is much faster than methods converting number to string as follows
        //      var tmp = val.toString();
        //      return tmp.length - 1 - tmp.indexOf('.');
        // especially when precision is low
        var e = 1;
        var count = 0;
        while (Math.round(val * e) / e !== val) {
            e *= 10;
            count++;
        }
        return count;
    };

    /**
     * @param {string|number} val
     * @return {number}
     */
    number.getPrecisionSafe = function (val) {
        var str = val.toString();

        // Consider scientific notation: '3.4e-12' '3.4e+12'
        var eIndex = str.indexOf('e');
        if (eIndex > 0) {
            var precision = +str.slice(eIndex + 1);
            return precision < 0 ? -precision : 0;
        }
        else {
            var dotIndex = str.indexOf('.');
            return dotIndex < 0 ? 0 : str.length - 1 - dotIndex;
        }
    };

    /**
     * Minimal dicernible data precisioin according to a single pixel.
     *
     * @param {Array.<number>} dataExtent
     * @param {Array.<number>} pixelExtent
     * @return {number} precision
     */
    number.getPixelPrecision = function (dataExtent, pixelExtent) {
        var log = Math.log;
        var LN10 = Math.LN10;
        var dataQuantity = Math.floor(log(dataExtent[1] - dataExtent[0]) / LN10);
        var sizeQuantity = Math.round(log(Math.abs(pixelExtent[1] - pixelExtent[0])) / LN10);
        // toFixed() digits argument must be between 0 and 20.
        var precision = Math.min(Math.max(-dataQuantity + sizeQuantity, 0), 20);
        return !isFinite(precision) ? 20 : precision;
    };

    /**
     * Get a data of given precision, assuring the sum of percentages
     * in valueList is 1.
     * The largest remainer method is used.
     * https://en.wikipedia.org/wiki/Largest_remainder_method
     *
     * @param {Array.<number>} valueList a list of all data
     * @param {number} idx index of the data to be processed in valueList
     * @param {number} precision integer number showing digits of precision
     * @return {number} percent ranging from 0 to 100
     */
    number.getPercentWithPrecision = function (valueList, idx, precision) {
        if (!valueList[idx]) {
            return 0;
        }

        var sum = zrUtil.reduce(valueList, function (acc, val) {
            return acc + (isNaN(val) ? 0 : val);
        }, 0);
        if (sum === 0) {
            return 0;
        }

        var digits = Math.pow(10, precision);
        var votesPerQuota = zrUtil.map(valueList, function (val) {
            return (isNaN(val) ? 0 : val) / sum * digits * 100;
        });
        var targetSeats = digits * 100;

        var seats = zrUtil.map(votesPerQuota, function (votes) {
            // Assign automatic seats.
            return Math.floor(votes);
        });
        var currentSum = zrUtil.reduce(seats, function (acc, val) {
            return acc + val;
        }, 0);

        var remainder = zrUtil.map(votesPerQuota, function (votes, idx) {
            return votes - seats[idx];
        });

        // Has remainding votes.
        while (currentSum < targetSeats) {
            // Find next largest remainder.
            var max = Number.NEGATIVE_INFINITY;
            var maxId = null;
            for (var i = 0, len = remainder.length; i < len; ++i) {
                if (remainder[i] > max) {
                    max = remainder[i];
                    maxId = i;
                }
            }

            // Add a vote to max remainder.
            ++seats[maxId];
            remainder[maxId] = 0;
            ++currentSum;
        }

        return seats[idx] / digits;
    };

    // Number.MAX_SAFE_INTEGER, ie do not support.
    number.MAX_SAFE_INTEGER = 9007199254740991;

    /**
     * To 0 - 2 * PI, considering negative radian.
     * @param {number} radian
     * @return {number}
     */
    number.remRadian = function (radian) {
        var pi2 = Math.PI * 2;
        return (radian % pi2 + pi2) % pi2;
    };

    /**
     * @param {type} radian
     * @return {boolean}
     */
    number.isRadianAroundZero = function (val) {
        return val > -RADIAN_EPSILON && val < RADIAN_EPSILON;
    };

    var TIME_REG = /^(?:(\d{4})(?:[-\/](\d{1,2})(?:[-\/](\d{1,2})(?:[T ](\d{1,2})(?::(\d\d)(?::(\d\d)(?:[.,](\d+))?)?)?(Z|[\+\-]\d\d:?\d\d)?)?)?)?)?$/; // jshint ignore:line

    /**
     * @return {number} in minutes
     */
    number.getTimezoneOffset = function () {
        return (new Date()).getTimezoneOffset();
    };

    /**
     * @param {string|Date|number} value These values can be accepted:
     *   + An instance of Date, represent a time in its own time zone.
     *   + Or string in a subset of ISO 8601, only including:
     *     + only year, month, date: '2012-03', '2012-03-01', '2012-03-01 05', '2012-03-01 05:06',
     *     + separated with T or space: '2012-03-01T12:22:33.123', '2012-03-01 12:22:33.123',
     *     + time zone: '2012-03-01T12:22:33Z', '2012-03-01T12:22:33+8000', '2012-03-01T12:22:33-05:00',
     *     all of which will be treated as local time if time zone is not specified
     *     (see <https://momentjs.com/>).
     *   + Or other string format, including (all of which will be treated as loacal time):
     *     '2012', '2012-3-1', '2012/3/1', '2012/03/01',
     *     '2009/6/12 2:00', '2009/6/12 2:05:08', '2009/6/12 2:05:08.123'
     *   + a timestamp, which represent a time in UTC.
     * @return {Date} date
     */
    number.parseDate = function (value) {
        if (value instanceof Date) {
            return value;
        }
        else if (typeof value === 'string') {
            // Different browsers parse date in different way, so we parse it manually.
            // Some other issues:
            // new Date('1970-01-01') is UTC,
            // new Date('1970/01/01') and new Date('1970-1-01') is local.
            // See issue #3623
            var match = TIME_REG.exec(value);

            if (!match) {
                // return Invalid Date.
                return new Date(NaN);
            }

            var timezoneOffset = number.getTimezoneOffset();
            var timeOffset = !match[8]
                ? 0
                : match[8].toUpperCase() === 'Z'
                ? timezoneOffset
                : +match[8].slice(0, 3) * 60 + timezoneOffset;

            // match[n] can only be string or undefined.
            // But take care of '12' + 1 => '121'.
            return new Date(
                +match[1],
                +(match[2] || 1) - 1,
                +match[3] || 1,
                +match[4] || 0,
                +(match[5] || 0) - timeOffset,
                +match[6] || 0,
                +match[7] || 0
            );
        }
        else if (value == null) {
            return new Date(NaN);
        }

        return new Date(Math.round(value));
    };

    /**
     * Quantity of a number. e.g. 0.1, 1, 10, 100
     *
     * @param  {number} val
     * @return {number}
     */
    number.quantity = function (val) {
        return Math.pow(10, quantityExponent(val));
    };

    function quantityExponent(val) {
        return Math.floor(Math.log(val) / Math.LN10);
    }

    /**
     * find a “nice” number approximately equal to x. Round the number if round = true,
     * take ceiling if round = false. The primary observation is that the “nicest”
     * numbers in decimal are 1, 2, and 5, and all power-of-ten multiples of these numbers.
     *
     * See "Nice Numbers for Graph Labels" of Graphic Gems.
     *
     * @param  {number} val Non-negative value.
     * @param  {boolean} round
     * @return {number}
     */
    number.nice = function (val, round) {
        var exponent = quantityExponent(val);
        var exp10 = Math.pow(10, exponent);
        var f = val / exp10; // 1 <= f < 10
        var nf;
        if (round) {
            if (f < 1.5) { nf = 1; }
            else if (f < 2.5) { nf = 2; }
            else if (f < 4) { nf = 3; }
            else if (f < 7) { nf = 5; }
            else { nf = 10; }
        }
        else {
            if (f < 1) { nf = 1; }
            else if (f < 2) { nf = 2; }
            else if (f < 3) { nf = 3; }
            else if (f < 5) { nf = 5; }
            else { nf = 10; }
        }
        val = nf * exp10;

        // Fix 3 * 0.1 === 0.30000000000000004 issue (see IEEE 754).
        // 20 is the uppper bound of toFixed.
        return exponent >= -20 ? +val.toFixed(exponent < 0 ? -exponent : 0) : val;
    };

    /**
     * Order intervals asc, and split them when overlap.
     * expect(numberUtil.reformIntervals([
     *     {interval: [18, 62], close: [1, 1]},
     *     {interval: [-Infinity, -70], close: [0, 0]},
     *     {interval: [-70, -26], close: [1, 1]},
     *     {interval: [-26, 18], close: [1, 1]},
     *     {interval: [62, 150], close: [1, 1]},
     *     {interval: [106, 150], close: [1, 1]},
     *     {interval: [150, Infinity], close: [0, 0]}
     * ])).toEqual([
     *     {interval: [-Infinity, -70], close: [0, 0]},
     *     {interval: [-70, -26], close: [1, 1]},
     *     {interval: [-26, 18], close: [0, 1]},
     *     {interval: [18, 62], close: [0, 1]},
     *     {interval: [62, 150], close: [0, 1]},
     *     {interval: [150, Infinity], close: [0, 0]}
     * ]);
     * @param {Array.<Object>} list, where `close` mean open or close
     *        of the interval, and Infinity can be used.
     * @return {Array.<Object>} The origin list, which has been reformed.
     */
    number.reformIntervals = function (list) {
        list.sort(function (a, b) {
            return littleThan(a, b, 0) ? -1 : 1;
        });

        var curr = -Infinity;
        var currClose = 1;
        for (var i = 0; i < list.length;) {
            var interval = list[i].interval;
            var close = list[i].close;

            for (var lg = 0; lg < 2; lg++) {
                if (interval[lg] <= curr) {
                    interval[lg] = curr;
                    close[lg] = !lg ? 1 - currClose : 1;
                }
                curr = interval[lg];
                currClose = close[lg];
            }

            if (interval[0] === interval[1] && close[0] * close[1] !== 1) {
                list.splice(i, 1);
            }
            else {
                i++;
            }
        }

        return list;

        function littleThan(a, b, lg) {
            return a.interval[lg] < b.interval[lg]
                || (
                    a.interval[lg] === b.interval[lg]
                    && (
                        (a.close[lg] - b.close[lg] === (!lg ? 1 : -1))
                        || (!lg && littleThan(a, b, 1))
                    )
                );
        }
    };

    /**
     * parseFloat NaNs numeric-cast false positives (null|true|false|"")
     * ...but misinterprets leading-number strings, particularly hex literals ("0x...")
     * subtraction forces infinities to NaN
     *
     * @param {*} v
     * @return {boolean}
     */
    number.isNumeric = function (v) {
        return v - parseFloat(v) >= 0;
    };

    module.exports = number;


/***/ }),
/* 10 */
/***/ (function(module, exports, __webpack_require__) {

// TODO Parse shadow style
// TODO Only shallow path support

    var zrUtil = __webpack_require__(0);

    module.exports = function (properties) {
        // Normalize
        for (var i = 0; i < properties.length; i++) {
            if (!properties[i][1]) {
               properties[i][1] = properties[i][0];
            }
        }
        return function (excludes, includes) {
            var style = {};
            for (var i = 0; i < properties.length; i++) {
                var propName = properties[i][1];
                if ((excludes && zrUtil.indexOf(excludes, propName) >= 0)
                    || (includes && zrUtil.indexOf(includes, propName) < 0)
                ) {
                    continue;
                }
                var val = this.getShallow(propName);
                if (val != null) {
                    style[properties[i][0]] = val;
                }
            }
            return style;
        };
    };


/***/ }),
/* 11 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 可绘制的图形基类
 * Base class of all displayable graphic objects
 * @module zrender/graphic/Displayable
 */



    var zrUtil = __webpack_require__(0);

    var Style = __webpack_require__(35);

    var Element = __webpack_require__(14);
    var RectText = __webpack_require__(43);
    // var Stateful = require('./mixin/Stateful');

    /**
     * @alias module:zrender/graphic/Displayable
     * @extends module:zrender/Element
     * @extends module:zrender/graphic/mixin/RectText
     */
    function Displayable(opts) {

        opts = opts || {};

        Element.call(this, opts);

        // Extend properties
        for (var name in opts) {
            if (
                opts.hasOwnProperty(name) &&
                name !== 'style'
            ) {
                this[name] = opts[name];
            }
        }

        /**
         * @type {module:zrender/graphic/Style}
         */
        this.style = new Style(opts.style);

        this._rect = null;
        // Shapes for cascade clipping.
        this.__clipPaths = [];

        // FIXME Stateful must be mixined after style is setted
        // Stateful.call(this, opts);
    }

    Displayable.prototype = {

        constructor: Displayable,

        type: 'displayable',

        /**
         * Displayable 是否为脏，Painter 中会根据该标记判断是否需要是否需要重新绘制
         * Dirty flag. From which painter will determine if this displayable object needs brush
         * @name module:zrender/graphic/Displayable#__dirty
         * @type {boolean}
         */
        __dirty: true,

        /**
         * 图形是否可见，为true时不绘制图形，但是仍能触发鼠标事件
         * If ignore drawing of the displayable object. Mouse event will still be triggered
         * @name module:/zrender/graphic/Displayable#invisible
         * @type {boolean}
         * @default false
         */
        invisible: false,

        /**
         * @name module:/zrender/graphic/Displayable#z
         * @type {number}
         * @default 0
         */
        z: 0,

        /**
         * @name module:/zrender/graphic/Displayable#z
         * @type {number}
         * @default 0
         */
        z2: 0,

        /**
         * z层level，决定绘画在哪层canvas中
         * @name module:/zrender/graphic/Displayable#zlevel
         * @type {number}
         * @default 0
         */
        zlevel: 0,

        /**
         * 是否可拖拽
         * @name module:/zrender/graphic/Displayable#draggable
         * @type {boolean}
         * @default false
         */
        draggable: false,

        /**
         * 是否正在拖拽
         * @name module:/zrender/graphic/Displayable#draggable
         * @type {boolean}
         * @default false
         */
        dragging: false,

        /**
         * 是否相应鼠标事件
         * @name module:/zrender/graphic/Displayable#silent
         * @type {boolean}
         * @default false
         */
        silent: false,

        /**
         * If enable culling
         * @type {boolean}
         * @default false
         */
        culling: false,

        /**
         * Mouse cursor when hovered
         * @name module:/zrender/graphic/Displayable#cursor
         * @type {string}
         */
        cursor: 'pointer',

        /**
         * If hover area is bounding rect
         * @name module:/zrender/graphic/Displayable#rectHover
         * @type {string}
         */
        rectHover: false,

        /**
         * Render the element progressively when the value >= 0,
         * usefull for large data.
         * @type {number}
         */
        progressive: -1,

        beforeBrush: function (ctx) {},

        afterBrush: function (ctx) {},

        /**
         * 图形绘制方法
         * @param {Canvas2DRenderingContext} ctx
         */
        // Interface
        brush: function (ctx, prevEl) {},

        /**
         * 获取最小包围盒
         * @return {module:zrender/core/BoundingRect}
         */
        // Interface
        getBoundingRect: function () {},

        /**
         * 判断坐标 x, y 是否在图形上
         * If displayable element contain coord x, y
         * @param  {number} x
         * @param  {number} y
         * @return {boolean}
         */
        contain: function (x, y) {
            return this.rectContain(x, y);
        },

        /**
         * @param  {Function} cb
         * @param  {}   context
         */
        traverse: function (cb, context) {
            cb.call(context, this);
        },

        /**
         * 判断坐标 x, y 是否在图形的包围盒上
         * If bounding rect of element contain coord x, y
         * @param  {number} x
         * @param  {number} y
         * @return {boolean}
         */
        rectContain: function (x, y) {
            var coord = this.transformCoordToLocal(x, y);
            var rect = this.getBoundingRect();
            return rect.contain(coord[0], coord[1]);
        },

        /**
         * 标记图形元素为脏，并且在下一帧重绘
         * Mark displayable element dirty and refresh next frame
         */
        dirty: function () {
            this.__dirty = true;

            this._rect = null;

            this.__zr && this.__zr.refresh();
        },

        /**
         * 图形是否会触发事件
         * If displayable object binded any event
         * @return {boolean}
         */
        // TODO, 通过 bind 绑定的事件
        // isSilent: function () {
        //     return !(
        //         this.hoverable || this.draggable
        //         || this.onmousemove || this.onmouseover || this.onmouseout
        //         || this.onmousedown || this.onmouseup || this.onclick
        //         || this.ondragenter || this.ondragover || this.ondragleave
        //         || this.ondrop
        //     );
        // },
        /**
         * Alias for animate('style')
         * @param {boolean} loop
         */
        animateStyle: function (loop) {
            return this.animate('style', loop);
        },

        attrKV: function (key, value) {
            if (key !== 'style') {
                Element.prototype.attrKV.call(this, key, value);
            }
            else {
                this.style.set(value);
            }
        },

        /**
         * @param {Object|string} key
         * @param {*} value
         */
        setStyle: function (key, value) {
            this.style.set(key, value);
            this.dirty(false);
            return this;
        },

        /**
         * Use given style object
         * @param  {Object} obj
         */
        useStyle: function (obj) {
            this.style = new Style(obj);
            this.dirty(false);
            return this;
        }
    };

    zrUtil.inherits(Displayable, Element);

    zrUtil.mixin(Displayable, RectText);
    // zrUtil.mixin(Displayable, Stateful);

    module.exports = Displayable;


/***/ }),
/* 12 */
/***/ (function(module, exports, __webpack_require__) {



    var zrUtil = __webpack_require__(0);
    var numberUtil = __webpack_require__(9);
    var textContain = __webpack_require__(5);

    var formatUtil = {};

    /**
     * 每三位默认加,格式化
     * @param {string|number} x
     * @return {string}
     */
    formatUtil.addCommas = function (x) {
        if (isNaN(x)) {
            return '-';
        }
        x = (x + '').split('.');
        return x[0].replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g,'$1,')
               + (x.length > 1 ? ('.' + x[1]) : '');
    };

    /**
     * @param {string} str
     * @param {boolean} [upperCaseFirst=false]
     * @return {string} str
     */
    formatUtil.toCamelCase = function (str, upperCaseFirst) {
        str = (str || '').toLowerCase().replace(/-(.)/g, function(match, group1) {
            return group1.toUpperCase();
        });

        if (upperCaseFirst && str) {
            str = str.charAt(0).toUpperCase() + str.slice(1);
        }

        return str;
    };

    /**
     * Normalize css liked array configuration
     * e.g.
     *  3 => [3, 3, 3, 3]
     *  [4, 2] => [4, 2, 4, 2]
     *  [4, 3, 2] => [4, 3, 2, 3]
     * @param {number|Array.<number>} val
     */
    formatUtil.normalizeCssArray = function (val) {
        var len = val.length;
        if (typeof (val) === 'number') {
            return [val, val, val, val];
        }
        else if (len === 2) {
            // vertical | horizontal
            return [val[0], val[1], val[0], val[1]];
        }
        else if (len === 3) {
            // top | horizontal | bottom
            return [val[0], val[1], val[2], val[1]];
        }
        return val;
    };

    var encodeHTML = formatUtil.encodeHTML = function (source) {
        return String(source)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    };

    var TPL_VAR_ALIAS = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];

    var wrapVar = function (varName, seriesIdx) {
        return '{' + varName + (seriesIdx == null ? '' : seriesIdx) + '}';
    };

    /**
     * Template formatter
     * @param {string} tpl
     * @param {Array.<Object>|Object} paramsList
     * @param {boolean} [encode=false]
     * @return {string}
     */
    formatUtil.formatTpl = function (tpl, paramsList, encode) {
        if (!zrUtil.isArray(paramsList)) {
            paramsList = [paramsList];
        }
        var seriesLen = paramsList.length;
        if (!seriesLen) {
            return '';
        }

        var $vars = paramsList[0].$vars || [];
        for (var i = 0; i < $vars.length; i++) {
            var alias = TPL_VAR_ALIAS[i];
            var val = wrapVar(alias, 0);
            tpl = tpl.replace(wrapVar(alias), encode ? encodeHTML(val) : val);
        }
        for (var seriesIdx = 0; seriesIdx < seriesLen; seriesIdx++) {
            for (var k = 0; k < $vars.length; k++) {
                var val = paramsList[seriesIdx][$vars[k]];
                tpl = tpl.replace(
                    wrapVar(TPL_VAR_ALIAS[k], seriesIdx),
                    encode ? encodeHTML(val) : val
                );
            }
        }

        return tpl;
    };

    /**
     * simple Template formatter
     *
     * @param {string} tpl
     * @param {Object} param
     * @param {boolean} [encode=false]
     * @return {string}
     */
    formatUtil.formatTplSimple = function (tpl, param, encode) {
        zrUtil.each(param, function (value, key) {
            tpl = tpl.replace(
                '{' + key + '}',
                encode ? encodeHTML(value) : value
            );
        });
        return tpl;
    };

    /**
     * @param {string} color
     * @param {string} [extraCssText]
     * @return {string}
     */
    formatUtil.getTooltipMarker = function (color, extraCssText) {
        return color
            ? '<span style="display:inline-block;margin-right:5px;'
                + 'border-radius:10px;width:9px;height:9px;background-color:'
                + formatUtil.encodeHTML(color) + ';' + (extraCssText || '') + '"></span>'
            : '';
    };

    /**
     * @param {string} str
     * @return {string}
     * @inner
     */
    var s2d = function (str) {
        return str < 10 ? ('0' + str) : str;
    };

    /**
     * ISO Date format
     * @param {string} tpl
     * @param {number} value
     * @param {boolean} [isUTC=false] Default in local time.
     *           see `module:echarts/scale/Time`
     *           and `module:echarts/util/number#parseDate`.
     * @inner
     */
    formatUtil.formatTime = function (tpl, value, isUTC) {
        if (tpl === 'week'
            || tpl === 'month'
            || tpl === 'quarter'
            || tpl === 'half-year'
            || tpl === 'year'
        ) {
            tpl = 'MM-dd\nyyyy';
        }

        var date = numberUtil.parseDate(value);
        var utc = isUTC ? 'UTC' : '';
        var y = date['get' + utc + 'FullYear']();
        var M = date['get' + utc + 'Month']() + 1;
        var d = date['get' + utc + 'Date']();
        var h = date['get' + utc + 'Hours']();
        var m = date['get' + utc + 'Minutes']();
        var s = date['get' + utc + 'Seconds']();

        tpl = tpl.replace('MM', s2d(M))
            .toLowerCase()
            .replace('yyyy', y)
            .replace('yy', y % 100)
            .replace('dd', s2d(d))
            .replace('d', d)
            .replace('hh', s2d(h))
            .replace('h', h)
            .replace('mm', s2d(m))
            .replace('m', m)
            .replace('ss', s2d(s))
            .replace('s', s);

        return tpl;
    };

    /**
     * Capital first
     * @param {string} str
     * @return {string}
     */
    formatUtil.capitalFirst = function (str) {
        return str ? str.charAt(0).toUpperCase() + str.substr(1) : str;
    };

    formatUtil.truncateText = textContain.truncateText;

    module.exports = formatUtil;



/***/ }),
/* 13 */
/***/ (function(module, exports) {

/**
 * echarts设备环境识别
 *
 * @desc echarts基于Canvas，纯Javascript图表库，提供直观，生动，可交互，可个性化定制的数据统计图表。
 * @author firede[firede@firede.us]
 * @desc thanks zepto.
 */

    var env = {};
    if (typeof navigator === 'undefined') {
        // In node
        env = {
            browser: {},
            os: {},
            node: true,
            // Assume canvas is supported
            canvasSupported: true
        };
    }
    else {
        env = detect(navigator.userAgent);
    }

    module.exports = env;

    // Zepto.js
    // (c) 2010-2013 Thomas Fuchs
    // Zepto.js may be freely distributed under the MIT license.

    function detect(ua) {
        var os = {};
        var browser = {};
        // var webkit = ua.match(/Web[kK]it[\/]{0,1}([\d.]+)/);
        // var android = ua.match(/(Android);?[\s\/]+([\d.]+)?/);
        // var ipad = ua.match(/(iPad).*OS\s([\d_]+)/);
        // var ipod = ua.match(/(iPod)(.*OS\s([\d_]+))?/);
        // var iphone = !ipad && ua.match(/(iPhone\sOS)\s([\d_]+)/);
        // var webos = ua.match(/(webOS|hpwOS)[\s\/]([\d.]+)/);
        // var touchpad = webos && ua.match(/TouchPad/);
        // var kindle = ua.match(/Kindle\/([\d.]+)/);
        // var silk = ua.match(/Silk\/([\d._]+)/);
        // var blackberry = ua.match(/(BlackBerry).*Version\/([\d.]+)/);
        // var bb10 = ua.match(/(BB10).*Version\/([\d.]+)/);
        // var rimtabletos = ua.match(/(RIM\sTablet\sOS)\s([\d.]+)/);
        // var playbook = ua.match(/PlayBook/);
        // var chrome = ua.match(/Chrome\/([\d.]+)/) || ua.match(/CriOS\/([\d.]+)/);
        var firefox = ua.match(/Firefox\/([\d.]+)/);
        // var safari = webkit && ua.match(/Mobile\//) && !chrome;
        // var webview = ua.match(/(iPhone|iPod|iPad).*AppleWebKit(?!.*Safari)/) && !chrome;
        var ie = ua.match(/MSIE\s([\d.]+)/)
            // IE 11 Trident/7.0; rv:11.0
            || ua.match(/Trident\/.+?rv:(([\d.]+))/);
        var edge = ua.match(/Edge\/([\d.]+)/); // IE 12 and 12+

        var weChat = (/micromessenger/i).test(ua);

        // Todo: clean this up with a better OS/browser seperation:
        // - discern (more) between multiple browsers on android
        // - decide if kindle fire in silk mode is android or not
        // - Firefox on Android doesn't specify the Android version
        // - possibly devide in os, device and browser hashes

        // if (browser.webkit = !!webkit) browser.version = webkit[1];

        // if (android) os.android = true, os.version = android[2];
        // if (iphone && !ipod) os.ios = os.iphone = true, os.version = iphone[2].replace(/_/g, '.');
        // if (ipad) os.ios = os.ipad = true, os.version = ipad[2].replace(/_/g, '.');
        // if (ipod) os.ios = os.ipod = true, os.version = ipod[3] ? ipod[3].replace(/_/g, '.') : null;
        // if (webos) os.webos = true, os.version = webos[2];
        // if (touchpad) os.touchpad = true;
        // if (blackberry) os.blackberry = true, os.version = blackberry[2];
        // if (bb10) os.bb10 = true, os.version = bb10[2];
        // if (rimtabletos) os.rimtabletos = true, os.version = rimtabletos[2];
        // if (playbook) browser.playbook = true;
        // if (kindle) os.kindle = true, os.version = kindle[1];
        // if (silk) browser.silk = true, browser.version = silk[1];
        // if (!silk && os.android && ua.match(/Kindle Fire/)) browser.silk = true;
        // if (chrome) browser.chrome = true, browser.version = chrome[1];
        if (firefox) {
            browser.firefox = true;
            browser.version = firefox[1];
        }
        // if (safari && (ua.match(/Safari/) || !!os.ios)) browser.safari = true;
        // if (webview) browser.webview = true;

        if (ie) {
            browser.ie = true;
            browser.version = ie[1];
        }

        if (edge) {
            browser.edge = true;
            browser.version = edge[1];
        }

        // It is difficult to detect WeChat in Win Phone precisely, because ua can
        // not be set on win phone. So we do not consider Win Phone.
        if (weChat) {
            browser.weChat = true;
        }

        // os.tablet = !!(ipad || playbook || (android && !ua.match(/Mobile/)) ||
        //     (firefox && ua.match(/Tablet/)) || (ie && !ua.match(/Phone/) && ua.match(/Touch/)));
        // os.phone  = !!(!os.tablet && !os.ipod && (android || iphone || webos ||
        //     (chrome && ua.match(/Android/)) || (chrome && ua.match(/CriOS\/([\d.]+)/)) ||
        //     (firefox && ua.match(/Mobile/)) || (ie && ua.match(/Touch/))));

        return {
            browser: browser,
            os: os,
            node: false,
            // 原生canvas支持，改极端点了
            // canvasSupported : !(browser.ie && parseFloat(browser.version) < 9)
            canvasSupported : document.createElement('canvas').getContext ? true : false,
            // @see <http://stackoverflow.com/questions/4817029/whats-the-best-way-to-detect-a-touch-screen-device-using-javascript>
            // works on most browsers
            // IE10/11 does not support touch event, and MS Edge supports them but not by
            // default, so we dont check navigator.maxTouchPoints for them here.
            touchEventsSupported: 'ontouchstart' in window && !browser.ie && !browser.edge,
            // <http://caniuse.com/#search=pointer%20event>.
            pointerEventsSupported: 'onpointerdown' in window
                // Firefox supports pointer but not by default, only MS browsers are reliable on pointer
                // events currently. So we dont use that on other browsers unless tested sufficiently.
                // Although IE 10 supports pointer event, it use old style and is different from the
                // standard. So we exclude that. (IE 10 is hardly used on touch device)
                && (browser.edge || (browser.ie && browser.version >= 11))
        };
    }


/***/ }),
/* 14 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * @module zrender/Element
 */


    var guid = __webpack_require__(36);
    var Eventful = __webpack_require__(37);
    var Transformable = __webpack_require__(15);
    var Animatable = __webpack_require__(38);
    var zrUtil = __webpack_require__(0);

    /**
     * @alias module:zrender/Element
     * @constructor
     * @extends {module:zrender/mixin/Animatable}
     * @extends {module:zrender/mixin/Transformable}
     * @extends {module:zrender/mixin/Eventful}
     */
    var Element = function (opts) {

        Transformable.call(this, opts);
        Eventful.call(this, opts);
        Animatable.call(this, opts);

        /**
         * 画布元素ID
         * @type {string}
         */
        this.id = opts.id || guid();
    };

    Element.prototype = {

        /**
         * 元素类型
         * Element type
         * @type {string}
         */
        type: 'element',

        /**
         * 元素名字
         * Element name
         * @type {string}
         */
        name: '',

        /**
         * ZRender 实例对象，会在 element 添加到 zrender 实例中后自动赋值
         * ZRender instance will be assigned when element is associated with zrender
         * @name module:/zrender/Element#__zr
         * @type {module:zrender/ZRender}
         */
        __zr: null,

        /**
         * 图形是否忽略，为true时忽略图形的绘制以及事件触发
         * If ignore drawing and events of the element object
         * @name module:/zrender/Element#ignore
         * @type {boolean}
         * @default false
         */
        ignore: false,

        /**
         * 用于裁剪的路径(shape)，所有 Group 内的路径在绘制时都会被这个路径裁剪
         * 该路径会继承被裁减对象的变换
         * @type {module:zrender/graphic/Path}
         * @see http://www.w3.org/TR/2dcontext/#clipping-region
         * @readOnly
         */
        clipPath: null,

        /**
         * Drift element
         * @param  {number} dx dx on the global space
         * @param  {number} dy dy on the global space
         */
        drift: function (dx, dy) {
            switch (this.draggable) {
                case 'horizontal':
                    dy = 0;
                    break;
                case 'vertical':
                    dx = 0;
                    break;
            }

            var m = this.transform;
            if (!m) {
                m = this.transform = [1, 0, 0, 1, 0, 0];
            }
            m[4] += dx;
            m[5] += dy;

            this.decomposeTransform();
            this.dirty(false);
        },

        /**
         * Hook before update
         */
        beforeUpdate: function () {},
        /**
         * Hook after update
         */
        afterUpdate: function () {},
        /**
         * Update each frame
         */
        update: function () {
            this.updateTransform();
        },

        /**
         * @param  {Function} cb
         * @param  {}   context
         */
        traverse: function (cb, context) {},

        /**
         * @protected
         */
        attrKV: function (key, value) {
            if (key === 'position' || key === 'scale' || key === 'origin') {
                // Copy the array
                if (value) {
                    var target = this[key];
                    if (!target) {
                        target = this[key] = [];
                    }
                    target[0] = value[0];
                    target[1] = value[1];
                }
            }
            else {
                this[key] = value;
            }
        },

        /**
         * Hide the element
         */
        hide: function () {
            this.ignore = true;
            this.__zr && this.__zr.refresh();
        },

        /**
         * Show the element
         */
        show: function () {
            this.ignore = false;
            this.__zr && this.__zr.refresh();
        },

        /**
         * @param {string|Object} key
         * @param {*} value
         */
        attr: function (key, value) {
            if (typeof key === 'string') {
                this.attrKV(key, value);
            }
            else if (zrUtil.isObject(key)) {
                for (var name in key) {
                    if (key.hasOwnProperty(name)) {
                        this.attrKV(name, key[name]);
                    }
                }
            }

            this.dirty(false);

            return this;
        },

        /**
         * @param {module:zrender/graphic/Path} clipPath
         */
        setClipPath: function (clipPath) {
            var zr = this.__zr;
            if (zr) {
                clipPath.addSelfToZr(zr);
            }

            // Remove previous clip path
            if (this.clipPath && this.clipPath !== clipPath) {
                this.removeClipPath();
            }

            this.clipPath = clipPath;
            clipPath.__zr = zr;
            clipPath.__clipTarget = this;

            this.dirty(false);
        },

        /**
         */
        removeClipPath: function () {
            var clipPath = this.clipPath;
            if (clipPath) {
                if (clipPath.__zr) {
                    clipPath.removeSelfFromZr(clipPath.__zr);
                }

                clipPath.__zr = null;
                clipPath.__clipTarget = null;
                this.clipPath = null;

                this.dirty(false);
            }
        },

        /**
         * Add self from zrender instance.
         * Not recursively because it will be invoked when element added to storage.
         * @param {module:zrender/ZRender} zr
         */
        addSelfToZr: function (zr) {
            this.__zr = zr;
            // 添加动画
            var animators = this.animators;
            if (animators) {
                for (var i = 0; i < animators.length; i++) {
                    zr.animation.addAnimator(animators[i]);
                }
            }

            if (this.clipPath) {
                this.clipPath.addSelfToZr(zr);
            }
        },

        /**
         * Remove self from zrender instance.
         * Not recursively because it will be invoked when element added to storage.
         * @param {module:zrender/ZRender} zr
         */
        removeSelfFromZr: function (zr) {
            this.__zr = null;
            // 移除动画
            var animators = this.animators;
            if (animators) {
                for (var i = 0; i < animators.length; i++) {
                    zr.animation.removeAnimator(animators[i]);
                }
            }

            if (this.clipPath) {
                this.clipPath.removeSelfFromZr(zr);
            }
        }
    };

    zrUtil.mixin(Element, Animatable);
    zrUtil.mixin(Element, Transformable);
    zrUtil.mixin(Element, Eventful);

    module.exports = Element;


/***/ }),
/* 15 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * 提供变换扩展
 * @module zrender/mixin/Transformable
 * @author pissang (https://www.github.com/pissang)
 */


    var matrix = __webpack_require__(8);
    var vector = __webpack_require__(2);
    var mIdentity = matrix.identity;

    var EPSILON = 5e-5;

    function isNotAroundZero(val) {
        return val > EPSILON || val < -EPSILON;
    }

    /**
     * @alias module:zrender/mixin/Transformable
     * @constructor
     */
    var Transformable = function (opts) {
        opts = opts || {};
        // If there are no given position, rotation, scale
        if (!opts.position) {
            /**
             * 平移
             * @type {Array.<number>}
             * @default [0, 0]
             */
            this.position = [0, 0];
        }
        if (opts.rotation == null) {
            /**
             * 旋转
             * @type {Array.<number>}
             * @default 0
             */
            this.rotation = 0;
        }
        if (!opts.scale) {
            /**
             * 缩放
             * @type {Array.<number>}
             * @default [1, 1]
             */
            this.scale = [1, 1];
        }
        /**
         * 旋转和缩放的原点
         * @type {Array.<number>}
         * @default null
         */
        this.origin = this.origin || null;
    };

    var transformableProto = Transformable.prototype;
    transformableProto.transform = null;

    /**
     * 判断是否需要有坐标变换
     * 如果有坐标变换, 则从position, rotation, scale以及父节点的transform计算出自身的transform矩阵
     */
    transformableProto.needLocalTransform = function () {
        return isNotAroundZero(this.rotation)
            || isNotAroundZero(this.position[0])
            || isNotAroundZero(this.position[1])
            || isNotAroundZero(this.scale[0] - 1)
            || isNotAroundZero(this.scale[1] - 1);
    };

    transformableProto.updateTransform = function () {
        var parent = this.parent;
        var parentHasTransform = parent && parent.transform;
        var needLocalTransform = this.needLocalTransform();

        var m = this.transform;
        if (!(needLocalTransform || parentHasTransform)) {
            m && mIdentity(m);
            return;
        }

        m = m || matrix.create();

        if (needLocalTransform) {
            this.getLocalTransform(m);
        }
        else {
            mIdentity(m);
        }

        // 应用父节点变换
        if (parentHasTransform) {
            if (needLocalTransform) {
                matrix.mul(m, parent.transform, m);
            }
            else {
                matrix.copy(m, parent.transform);
            }
        }
        // 保存这个变换矩阵
        this.transform = m;

        this.invTransform = this.invTransform || matrix.create();
        matrix.invert(this.invTransform, m);
    };

    transformableProto.getLocalTransform = function (m) {
        return Transformable.getLocalTransform(this, m);
    };

    /**
     * 将自己的transform应用到context上
     * @param {Context2D} ctx
     */
    transformableProto.setTransform = function (ctx) {
        var m = this.transform;
        var dpr = ctx.dpr || 1;
        if (m) {
            ctx.setTransform(dpr * m[0], dpr * m[1], dpr * m[2], dpr * m[3], dpr * m[4], dpr * m[5]);
        }
        else {
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }
    };

    transformableProto.restoreTransform = function (ctx) {
        var dpr = ctx.dpr || 1;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    var tmpTransform = [];

    /**
     * 分解`transform`矩阵到`position`, `rotation`, `scale`
     */
    transformableProto.decomposeTransform = function () {
        if (!this.transform) {
            return;
        }
        var parent = this.parent;
        var m = this.transform;
        if (parent && parent.transform) {
            // Get local transform and decompose them to position, scale, rotation
            matrix.mul(tmpTransform, parent.invTransform, m);
            m = tmpTransform;
        }
        var sx = m[0] * m[0] + m[1] * m[1];
        var sy = m[2] * m[2] + m[3] * m[3];
        var position = this.position;
        var scale = this.scale;
        if (isNotAroundZero(sx - 1)) {
            sx = Math.sqrt(sx);
        }
        if (isNotAroundZero(sy - 1)) {
            sy = Math.sqrt(sy);
        }
        if (m[0] < 0) {
            sx = -sx;
        }
        if (m[3] < 0) {
            sy = -sy;
        }
        position[0] = m[4];
        position[1] = m[5];
        scale[0] = sx;
        scale[1] = sy;
        this.rotation = Math.atan2(-m[1] / sy, m[0] / sx);
    };

    /**
     * Get global scale
     * @return {Array.<number>}
     */
    transformableProto.getGlobalScale = function () {
        var m = this.transform;
        if (!m) {
            return [1, 1];
        }
        var sx = Math.sqrt(m[0] * m[0] + m[1] * m[1]);
        var sy = Math.sqrt(m[2] * m[2] + m[3] * m[3]);
        if (m[0] < 0) {
            sx = -sx;
        }
        if (m[3] < 0) {
            sy = -sy;
        }
        return [sx, sy];
    };
    /**
     * 变换坐标位置到 shape 的局部坐标空间
     * @method
     * @param {number} x
     * @param {number} y
     * @return {Array.<number>}
     */
    transformableProto.transformCoordToLocal = function (x, y) {
        var v2 = [x, y];
        var invTransform = this.invTransform;
        if (invTransform) {
            vector.applyTransform(v2, v2, invTransform);
        }
        return v2;
    };

    /**
     * 变换局部坐标位置到全局坐标空间
     * @method
     * @param {number} x
     * @param {number} y
     * @return {Array.<number>}
     */
    transformableProto.transformCoordToGlobal = function (x, y) {
        var v2 = [x, y];
        var transform = this.transform;
        if (transform) {
            vector.applyTransform(v2, v2, transform);
        }
        return v2;
    };

    /**
     * @static
     * @param {Object} target
     * @param {Array.<number>} target.origin
     * @param {number} target.rotation
     * @param {Array.<number>} target.position
     * @param {Array.<number>} [m]
     */
    Transformable.getLocalTransform = function (target, m) {
        m = m || [];
        mIdentity(m);

        var origin = target.origin;
        var scale = target.scale || [1, 1];
        var rotation = target.rotation || 0;
        var position = target.position || [0, 0];

        if (origin) {
            // Translate to origin
            m[4] -= origin[0];
            m[5] -= origin[1];
        }
        matrix.scale(m, m, scale);
        if (rotation) {
            matrix.rotate(m, m, rotation);
        }
        if (origin) {
            // Translate back from origin
            m[4] += origin[0];
            m[5] += origin[1];
        }

        m[4] += position[0];
        m[5] += position[1];

        return m;
    };

    module.exports = Transformable;



/***/ }),
/* 16 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * @module zrender/tool/color
 */


    var LRU = __webpack_require__(17);

    var kCSSColorTable = {
        'transparent': [0,0,0,0], 'aliceblue': [240,248,255,1],
        'antiquewhite': [250,235,215,1], 'aqua': [0,255,255,1],
        'aquamarine': [127,255,212,1], 'azure': [240,255,255,1],
        'beige': [245,245,220,1], 'bisque': [255,228,196,1],
        'black': [0,0,0,1], 'blanchedalmond': [255,235,205,1],
        'blue': [0,0,255,1], 'blueviolet': [138,43,226,1],
        'brown': [165,42,42,1], 'burlywood': [222,184,135,1],
        'cadetblue': [95,158,160,1], 'chartreuse': [127,255,0,1],
        'chocolate': [210,105,30,1], 'coral': [255,127,80,1],
        'cornflowerblue': [100,149,237,1], 'cornsilk': [255,248,220,1],
        'crimson': [220,20,60,1], 'cyan': [0,255,255,1],
        'darkblue': [0,0,139,1], 'darkcyan': [0,139,139,1],
        'darkgoldenrod': [184,134,11,1], 'darkgray': [169,169,169,1],
        'darkgreen': [0,100,0,1], 'darkgrey': [169,169,169,1],
        'darkkhaki': [189,183,107,1], 'darkmagenta': [139,0,139,1],
        'darkolivegreen': [85,107,47,1], 'darkorange': [255,140,0,1],
        'darkorchid': [153,50,204,1], 'darkred': [139,0,0,1],
        'darksalmon': [233,150,122,1], 'darkseagreen': [143,188,143,1],
        'darkslateblue': [72,61,139,1], 'darkslategray': [47,79,79,1],
        'darkslategrey': [47,79,79,1], 'darkturquoise': [0,206,209,1],
        'darkviolet': [148,0,211,1], 'deeppink': [255,20,147,1],
        'deepskyblue': [0,191,255,1], 'dimgray': [105,105,105,1],
        'dimgrey': [105,105,105,1], 'dodgerblue': [30,144,255,1],
        'firebrick': [178,34,34,1], 'floralwhite': [255,250,240,1],
        'forestgreen': [34,139,34,1], 'fuchsia': [255,0,255,1],
        'gainsboro': [220,220,220,1], 'ghostwhite': [248,248,255,1],
        'gold': [255,215,0,1], 'goldenrod': [218,165,32,1],
        'gray': [128,128,128,1], 'green': [0,128,0,1],
        'greenyellow': [173,255,47,1], 'grey': [128,128,128,1],
        'honeydew': [240,255,240,1], 'hotpink': [255,105,180,1],
        'indianred': [205,92,92,1], 'indigo': [75,0,130,1],
        'ivory': [255,255,240,1], 'khaki': [240,230,140,1],
        'lavender': [230,230,250,1], 'lavenderblush': [255,240,245,1],
        'lawngreen': [124,252,0,1], 'lemonchiffon': [255,250,205,1],
        'lightblue': [173,216,230,1], 'lightcoral': [240,128,128,1],
        'lightcyan': [224,255,255,1], 'lightgoldenrodyellow': [250,250,210,1],
        'lightgray': [211,211,211,1], 'lightgreen': [144,238,144,1],
        'lightgrey': [211,211,211,1], 'lightpink': [255,182,193,1],
        'lightsalmon': [255,160,122,1], 'lightseagreen': [32,178,170,1],
        'lightskyblue': [135,206,250,1], 'lightslategray': [119,136,153,1],
        'lightslategrey': [119,136,153,1], 'lightsteelblue': [176,196,222,1],
        'lightyellow': [255,255,224,1], 'lime': [0,255,0,1],
        'limegreen': [50,205,50,1], 'linen': [250,240,230,1],
        'magenta': [255,0,255,1], 'maroon': [128,0,0,1],
        'mediumaquamarine': [102,205,170,1], 'mediumblue': [0,0,205,1],
        'mediumorchid': [186,85,211,1], 'mediumpurple': [147,112,219,1],
        'mediumseagreen': [60,179,113,1], 'mediumslateblue': [123,104,238,1],
        'mediumspringgreen': [0,250,154,1], 'mediumturquoise': [72,209,204,1],
        'mediumvioletred': [199,21,133,1], 'midnightblue': [25,25,112,1],
        'mintcream': [245,255,250,1], 'mistyrose': [255,228,225,1],
        'moccasin': [255,228,181,1], 'navajowhite': [255,222,173,1],
        'navy': [0,0,128,1], 'oldlace': [253,245,230,1],
        'olive': [128,128,0,1], 'olivedrab': [107,142,35,1],
        'orange': [255,165,0,1], 'orangered': [255,69,0,1],
        'orchid': [218,112,214,1], 'palegoldenrod': [238,232,170,1],
        'palegreen': [152,251,152,1], 'paleturquoise': [175,238,238,1],
        'palevioletred': [219,112,147,1], 'papayawhip': [255,239,213,1],
        'peachpuff': [255,218,185,1], 'peru': [205,133,63,1],
        'pink': [255,192,203,1], 'plum': [221,160,221,1],
        'powderblue': [176,224,230,1], 'purple': [128,0,128,1],
        'red': [255,0,0,1], 'rosybrown': [188,143,143,1],
        'royalblue': [65,105,225,1], 'saddlebrown': [139,69,19,1],
        'salmon': [250,128,114,1], 'sandybrown': [244,164,96,1],
        'seagreen': [46,139,87,1], 'seashell': [255,245,238,1],
        'sienna': [160,82,45,1], 'silver': [192,192,192,1],
        'skyblue': [135,206,235,1], 'slateblue': [106,90,205,1],
        'slategray': [112,128,144,1], 'slategrey': [112,128,144,1],
        'snow': [255,250,250,1], 'springgreen': [0,255,127,1],
        'steelblue': [70,130,180,1], 'tan': [210,180,140,1],
        'teal': [0,128,128,1], 'thistle': [216,191,216,1],
        'tomato': [255,99,71,1], 'turquoise': [64,224,208,1],
        'violet': [238,130,238,1], 'wheat': [245,222,179,1],
        'white': [255,255,255,1], 'whitesmoke': [245,245,245,1],
        'yellow': [255,255,0,1], 'yellowgreen': [154,205,50,1]
    };

    function clampCssByte(i) {  // Clamp to integer 0 .. 255.
        i = Math.round(i);  // Seems to be what Chrome does (vs truncation).
        return i < 0 ? 0 : i > 255 ? 255 : i;
    }

    function clampCssAngle(i) {  // Clamp to integer 0 .. 360.
        i = Math.round(i);  // Seems to be what Chrome does (vs truncation).
        return i < 0 ? 0 : i > 360 ? 360 : i;
    }

    function clampCssFloat(f) {  // Clamp to float 0.0 .. 1.0.
        return f < 0 ? 0 : f > 1 ? 1 : f;
    }

    function parseCssInt(str) {  // int or percentage.
        if (str.length && str.charAt(str.length - 1) === '%') {
            return clampCssByte(parseFloat(str) / 100 * 255);
        }
        return clampCssByte(parseInt(str, 10));
    }

    function parseCssFloat(str) {  // float or percentage.
        if (str.length && str.charAt(str.length - 1) === '%') {
            return clampCssFloat(parseFloat(str) / 100);
        }
        return clampCssFloat(parseFloat(str));
    }

    function cssHueToRgb(m1, m2, h) {
        if (h < 0) {
            h += 1;
        }
        else if (h > 1) {
            h -= 1;
        }

        if (h * 6 < 1) {
            return m1 + (m2 - m1) * h * 6;
        }
        if (h * 2 < 1) {
            return m2;
        }
        if (h * 3 < 2) {
            return m1 + (m2 - m1) * (2/3 - h) * 6;
        }
        return m1;
    }

    function lerp(a, b, p) {
        return a + (b - a) * p;
    }

    function setRgba(out, r, g, b, a) {
        out[0] = r; out[1] = g; out[2] = b; out[3] = a;
        return out;
    }
    function copyRgba(out, a) {
        out[0] = a[0]; out[1] = a[1]; out[2] = a[2]; out[3] = a[3];
        return out;
    }
    var colorCache = new LRU(20);
    var lastRemovedArr = null;
    function putToCache(colorStr, rgbaArr) {
        // Reuse removed array
        if (lastRemovedArr) {
            copyRgba(lastRemovedArr, rgbaArr);
        }
        lastRemovedArr = colorCache.put(colorStr, lastRemovedArr || (rgbaArr.slice()));
    }
    /**
     * @param {string} colorStr
     * @param {Array.<number>} out
     * @return {Array.<number>}
     * @memberOf module:zrender/util/color
     */
    function parse(colorStr, rgbaArr) {
        if (!colorStr) {
            return;
        }
        rgbaArr = rgbaArr || [];

        var cached = colorCache.get(colorStr);
        if (cached) {
            return copyRgba(rgbaArr, cached);
        }

        // colorStr may be not string
        colorStr = colorStr + '';
        // Remove all whitespace, not compliant, but should just be more accepting.
        var str = colorStr.replace(/ /g, '').toLowerCase();

        // Color keywords (and transparent) lookup.
        if (str in kCSSColorTable) {
            copyRgba(rgbaArr, kCSSColorTable[str]);
            putToCache(colorStr, rgbaArr);
            return rgbaArr;
        }

        // #abc and #abc123 syntax.
        if (str.charAt(0) === '#') {
            if (str.length === 4) {
                var iv = parseInt(str.substr(1), 16);  // TODO(deanm): Stricter parsing.
                if (!(iv >= 0 && iv <= 0xfff)) {
                    setRgba(rgbaArr, 0, 0, 0, 1);
                    return;  // Covers NaN.
                }
                setRgba(rgbaArr,
                    ((iv & 0xf00) >> 4) | ((iv & 0xf00) >> 8),
                    (iv & 0xf0) | ((iv & 0xf0) >> 4),
                    (iv & 0xf) | ((iv & 0xf) << 4),
                    1
                );
                putToCache(colorStr, rgbaArr);
                return rgbaArr;
            }
            else if (str.length === 7) {
                var iv = parseInt(str.substr(1), 16);  // TODO(deanm): Stricter parsing.
                if (!(iv >= 0 && iv <= 0xffffff)) {
                    setRgba(rgbaArr, 0, 0, 0, 1);
                    return;  // Covers NaN.
                }
                setRgba(rgbaArr,
                    (iv & 0xff0000) >> 16,
                    (iv & 0xff00) >> 8,
                    iv & 0xff,
                    1
                );
                putToCache(colorStr, rgbaArr);
                return rgbaArr;
            }

            return;
        }
        var op = str.indexOf('('), ep = str.indexOf(')');
        if (op !== -1 && ep + 1 === str.length) {
            var fname = str.substr(0, op);
            var params = str.substr(op + 1, ep - (op + 1)).split(',');
            var alpha = 1;  // To allow case fallthrough.
            switch (fname) {
                case 'rgba':
                    if (params.length !== 4) {
                        setRgba(rgbaArr, 0, 0, 0, 1);
                        return;
                    }
                    alpha = parseCssFloat(params.pop()); // jshint ignore:line
                // Fall through.
                case 'rgb':
                    if (params.length !== 3) {
                        setRgba(rgbaArr, 0, 0, 0, 1);
                        return;
                    }
                    setRgba(rgbaArr,
                        parseCssInt(params[0]),
                        parseCssInt(params[1]),
                        parseCssInt(params[2]),
                        alpha
                    );
                    putToCache(colorStr, rgbaArr);
                    return rgbaArr;
                case 'hsla':
                    if (params.length !== 4) {
                        setRgba(rgbaArr, 0, 0, 0, 1);
                        return;
                    }
                    params[3] = parseCssFloat(params[3]);
                    hsla2rgba(params, rgbaArr);
                    putToCache(colorStr, rgbaArr);
                    return rgbaArr;
                case 'hsl':
                    if (params.length !== 3) {
                        setRgba(rgbaArr, 0, 0, 0, 1);
                        return;
                    }
                    hsla2rgba(params, rgbaArr);
                    putToCache(colorStr, rgbaArr);
                    return rgbaArr;
                default:
                    return;
            }
        }

        setRgba(rgbaArr, 0, 0, 0, 1);
        return;
    }

    /**
     * @param {Array.<number>} hsla
     * @param {Array.<number>} rgba
     * @return {Array.<number>} rgba
     */
    function hsla2rgba(hsla, rgba) {
        var h = (((parseFloat(hsla[0]) % 360) + 360) % 360) / 360;  // 0 .. 1
        // NOTE(deanm): According to the CSS spec s/l should only be
        // percentages, but we don't bother and let float or percentage.
        var s = parseCssFloat(hsla[1]);
        var l = parseCssFloat(hsla[2]);
        var m2 = l <= 0.5 ? l * (s + 1) : l + s - l * s;
        var m1 = l * 2 - m2;

        rgba = rgba || [];
        setRgba(rgba,
            clampCssByte(cssHueToRgb(m1, m2, h + 1 / 3) * 255),
            clampCssByte(cssHueToRgb(m1, m2, h) * 255),
            clampCssByte(cssHueToRgb(m1, m2, h - 1 / 3) * 255),
            1
        );

        if (hsla.length === 4) {
            rgba[3] = hsla[3];
        }

        return rgba;
    }

    /**
     * @param {Array.<number>} rgba
     * @return {Array.<number>} hsla
     */
    function rgba2hsla(rgba) {
        if (!rgba) {
            return;
        }

        // RGB from 0 to 255
        var R = rgba[0] / 255;
        var G = rgba[1] / 255;
        var B = rgba[2] / 255;

        var vMin = Math.min(R, G, B); // Min. value of RGB
        var vMax = Math.max(R, G, B); // Max. value of RGB
        var delta = vMax - vMin; // Delta RGB value

        var L = (vMax + vMin) / 2;
        var H;
        var S;
        // HSL results from 0 to 1
        if (delta === 0) {
            H = 0;
            S = 0;
        }
        else {
            if (L < 0.5) {
                S = delta / (vMax + vMin);
            }
            else {
                S = delta / (2 - vMax - vMin);
            }

            var deltaR = (((vMax - R) / 6) + (delta / 2)) / delta;
            var deltaG = (((vMax - G) / 6) + (delta / 2)) / delta;
            var deltaB = (((vMax - B) / 6) + (delta / 2)) / delta;

            if (R === vMax) {
                H = deltaB - deltaG;
            }
            else if (G === vMax) {
                H = (1 / 3) + deltaR - deltaB;
            }
            else if (B === vMax) {
                H = (2 / 3) + deltaG - deltaR;
            }

            if (H < 0) {
                H += 1;
            }

            if (H > 1) {
                H -= 1;
            }
        }

        var hsla = [H * 360, S, L];

        if (rgba[3] != null) {
            hsla.push(rgba[3]);
        }

        return hsla;
    }

    /**
     * @param {string} color
     * @param {number} level
     * @return {string}
     * @memberOf module:zrender/util/color
     */
    function lift(color, level) {
        var colorArr = parse(color);
        if (colorArr) {
            for (var i = 0; i < 3; i++) {
                if (level < 0) {
                    colorArr[i] = colorArr[i] * (1 - level) | 0;
                }
                else {
                    colorArr[i] = ((255 - colorArr[i]) * level + colorArr[i]) | 0;
                }
            }
            return stringify(colorArr, colorArr.length === 4 ? 'rgba' : 'rgb');
        }
    }

    /**
     * @param {string} color
     * @return {string}
     * @memberOf module:zrender/util/color
     */
    function toHex(color, level) {
        var colorArr = parse(color);
        if (colorArr) {
            return ((1 << 24) + (colorArr[0] << 16) + (colorArr[1] << 8) + (+colorArr[2])).toString(16).slice(1);
        }
    }

    /**
     * Map value to color. Faster than mapToColor methods because color is represented by rgba array.
     * @param {number} normalizedValue A float between 0 and 1.
     * @param {Array.<Array.<number>>} colors List of rgba color array
     * @param {Array.<number>} [out] Mapped gba color array
     * @return {Array.<number>} will be null/undefined if input illegal.
     */
    function fastMapToColor(normalizedValue, colors, out) {
        if (!(colors && colors.length)
            || !(normalizedValue >= 0 && normalizedValue <= 1)
        ) {
            return;
        }

        out = out || [];

        var value = normalizedValue * (colors.length - 1);
        var leftIndex = Math.floor(value);
        var rightIndex = Math.ceil(value);
        var leftColor = colors[leftIndex];
        var rightColor = colors[rightIndex];
        var dv = value - leftIndex;
        out[0] = clampCssByte(lerp(leftColor[0], rightColor[0], dv));
        out[1] = clampCssByte(lerp(leftColor[1], rightColor[1], dv));
        out[2] = clampCssByte(lerp(leftColor[2], rightColor[2], dv));
        out[3] = clampCssFloat(lerp(leftColor[3], rightColor[3], dv));

        return out;
    }
    /**
     * @param {number} normalizedValue A float between 0 and 1.
     * @param {Array.<string>} colors Color list.
     * @param {boolean=} fullOutput Default false.
     * @return {(string|Object)} Result color. If fullOutput,
     *                           return {color: ..., leftIndex: ..., rightIndex: ..., value: ...},
     * @memberOf module:zrender/util/color
     */
    function mapToColor(normalizedValue, colors, fullOutput) {
        if (!(colors && colors.length)
            || !(normalizedValue >= 0 && normalizedValue <= 1)
        ) {
            return;
        }

        var value = normalizedValue * (colors.length - 1);
        var leftIndex = Math.floor(value);
        var rightIndex = Math.ceil(value);
        var leftColor = parse(colors[leftIndex]);
        var rightColor = parse(colors[rightIndex]);
        var dv = value - leftIndex;

        var color = stringify(
            [
                clampCssByte(lerp(leftColor[0], rightColor[0], dv)),
                clampCssByte(lerp(leftColor[1], rightColor[1], dv)),
                clampCssByte(lerp(leftColor[2], rightColor[2], dv)),
                clampCssFloat(lerp(leftColor[3], rightColor[3], dv))
            ],
            'rgba'
        );

        return fullOutput
            ? {
                color: color,
                leftIndex: leftIndex,
                rightIndex: rightIndex,
                value: value
            }
            : color;
    }

    /**
     * @param {string} color
     * @param {number=} h 0 ~ 360, ignore when null.
     * @param {number=} s 0 ~ 1, ignore when null.
     * @param {number=} l 0 ~ 1, ignore when null.
     * @return {string} Color string in rgba format.
     * @memberOf module:zrender/util/color
     */
    function modifyHSL(color, h, s, l) {
        color = parse(color);

        if (color) {
            color = rgba2hsla(color);
            h != null && (color[0] = clampCssAngle(h));
            s != null && (color[1] = parseCssFloat(s));
            l != null && (color[2] = parseCssFloat(l));

            return stringify(hsla2rgba(color), 'rgba');
        }
    }

    /**
     * @param {string} color
     * @param {number=} alpha 0 ~ 1
     * @return {string} Color string in rgba format.
     * @memberOf module:zrender/util/color
     */
    function modifyAlpha(color, alpha) {
        color = parse(color);

        if (color && alpha != null) {
            color[3] = clampCssFloat(alpha);
            return stringify(color, 'rgba');
        }
    }

    /**
     * @param {Array.<number>} arrColor like [12,33,44,0.4]
     * @param {string} type 'rgba', 'hsva', ...
     * @return {string} Result color. (If input illegal, return undefined).
     */
    function stringify(arrColor, type) {
        if (!arrColor || !arrColor.length) {
            return;
        }
        var colorStr = arrColor[0] + ',' + arrColor[1] + ',' + arrColor[2];
        if (type === 'rgba' || type === 'hsva' || type === 'hsla') {
            colorStr += ',' + arrColor[3];
        }
        return type + '(' + colorStr + ')';
    }

    module.exports = {
        parse: parse,
        lift: lift,
        toHex: toHex,
        fastMapToColor: fastMapToColor,
        mapToColor: mapToColor,
        modifyHSL: modifyHSL,
        modifyAlpha: modifyAlpha,
        stringify: stringify
    };




/***/ }),
/* 17 */
/***/ (function(module, exports) {

// Simple LRU cache use doubly linked list
// @module zrender/core/LRU


    /**
     * Simple double linked list. Compared with array, it has O(1) remove operation.
     * @constructor
     */
    var LinkedList = function () {

        /**
         * @type {module:zrender/core/LRU~Entry}
         */
        this.head = null;

        /**
         * @type {module:zrender/core/LRU~Entry}
         */
        this.tail = null;

        this._len = 0;
    };

    var linkedListProto = LinkedList.prototype;
    /**
     * Insert a new value at the tail
     * @param  {} val
     * @return {module:zrender/core/LRU~Entry}
     */
    linkedListProto.insert = function (val) {
        var entry = new Entry(val);
        this.insertEntry(entry);
        return entry;
    };

    /**
     * Insert an entry at the tail
     * @param  {module:zrender/core/LRU~Entry} entry
     */
    linkedListProto.insertEntry = function (entry) {
        if (!this.head) {
            this.head = this.tail = entry;
        }
        else {
            this.tail.next = entry;
            entry.prev = this.tail;
            entry.next = null;
            this.tail = entry;
        }
        this._len++;
    };

    /**
     * Remove entry.
     * @param  {module:zrender/core/LRU~Entry} entry
     */
    linkedListProto.remove = function (entry) {
        var prev = entry.prev;
        var next = entry.next;
        if (prev) {
            prev.next = next;
        }
        else {
            // Is head
            this.head = next;
        }
        if (next) {
            next.prev = prev;
        }
        else {
            // Is tail
            this.tail = prev;
        }
        entry.next = entry.prev = null;
        this._len--;
    };

    /**
     * @return {number}
     */
    linkedListProto.len = function () {
        return this._len;
    };

    /**
     * Clear list
     */
    linkedListProto.clear = function () {
        this.head = this.tail = null;
        this._len = 0;
    };

    /**
     * @constructor
     * @param {} val
     */
    var Entry = function (val) {
        /**
         * @type {}
         */
        this.value = val;

        /**
         * @type {module:zrender/core/LRU~Entry}
         */
        this.next;

        /**
         * @type {module:zrender/core/LRU~Entry}
         */
        this.prev;
    };

    /**
     * LRU Cache
     * @constructor
     * @alias module:zrender/core/LRU
     */
    var LRU = function (maxSize) {

        this._list = new LinkedList();

        this._map = {};

        this._maxSize = maxSize || 10;

        this._lastRemovedEntry = null;
    };

    var LRUProto = LRU.prototype;

    /**
     * @param  {string} key
     * @param  {} value
     * @return {} Removed value
     */
    LRUProto.put = function (key, value) {
        var list = this._list;
        var map = this._map;
        var removed = null;
        if (map[key] == null) {
            var len = list.len();
            // Reuse last removed entry
            var entry = this._lastRemovedEntry;

            if (len >= this._maxSize && len > 0) {
                // Remove the least recently used
                var leastUsedEntry = list.head;
                list.remove(leastUsedEntry);
                delete map[leastUsedEntry.key];

                removed = leastUsedEntry.value;
                this._lastRemovedEntry = leastUsedEntry;
            }

            if (entry) {
                entry.value = value;
            }
            else {
                entry = new Entry(value);
            }
            entry.key = key;
            list.insertEntry(entry);
            map[key] = entry;
        }

        return removed;
    };

    /**
     * @param  {string} key
     * @return {}
     */
    LRUProto.get = function (key) {
        var entry = this._map[key];
        var list = this._list;
        if (entry != null) {
            // Put the latest used entry in the tail
            if (entry !== list.tail) {
                list.remove(entry);
                list.insertEntry(entry);
            }

            return entry.value;
        }
    };

    /**
     * Clear the cache
     */
    LRUProto.clear = function () {
        this._list.clear();
        this._map = {};
    };

    module.exports = LRU;


/***/ }),
/* 18 */
/***/ (function(module, exports) {


    var dpr = 1;
    // If in browser environment
    if (typeof window !== 'undefined') {
        dpr = Math.max(window.devicePixelRatio || 1, 1);
    }
    /**
     * config默认配置项
     * @exports zrender/config
     * @author Kener (@Kener-林峰, kener.linfeng@gmail.com)
     */
    var config = {
        /**
         * debug日志选项：catchBrushException为true下有效
         * 0 : 不生成debug数据，发布用
         * 1 : 异常抛出，调试用
         * 2 : 控制台输出，调试用
         */
        debugMode: 0,

        // retina 屏幕优化
        devicePixelRatio: dpr
    };
    module.exports = config;




/***/ }),
/* 19 */
/***/ (function(module, exports) {



    var PI2 = Math.PI * 2;
    module.exports = {
        normalizeRadian: function(angle) {
            angle %= PI2;
            if (angle < 0) {
                angle += PI2;
            }
            return angle;
        }
    };


/***/ }),
/* 20 */
/***/ (function(module, exports, __webpack_require__) {



    var smoothSpline = __webpack_require__(60);
    var smoothBezier = __webpack_require__(61);

    module.exports = {
        buildPath: function (ctx, shape, closePath) {
            var points = shape.points;
            var smooth = shape.smooth;
            if (points && points.length >= 2) {
                if (smooth && smooth !== 'spline') {
                    var controlPoints = smoothBezier(
                        points, smooth, closePath, shape.smoothConstraint
                    );

                    ctx.moveTo(points[0][0], points[0][1]);
                    var len = points.length;
                    for (var i = 0; i < (closePath ? len : len - 1); i++) {
                        var cp1 = controlPoints[i * 2];
                        var cp2 = controlPoints[i * 2 + 1];
                        var p = points[(i + 1) % len];
                        ctx.bezierCurveTo(
                            cp1[0], cp1[1], cp2[0], cp2[1], p[0], p[1]
                        );
                    }
                }
                else {
                    if (smooth === 'spline') {
                        points = smoothSpline(points, closePath);
                    }

                    ctx.moveTo(points[0][0], points[0][1]);
                    for (var i = 1, l = points.length; i < l; i++) {
                        ctx.lineTo(points[i][0], points[i][1]);
                    }
                }

                closePath && ctx.closePath();
            }
        }
    };


/***/ }),
/* 21 */
/***/ (function(module, exports) {



    /**
     * @param {Array.<Object>} colorStops
     */
    var Gradient = function (colorStops) {

        this.colorStops = colorStops || [];
    };

    Gradient.prototype = {

        constructor: Gradient,

        addColorStop: function (offset, color) {
            this.colorStops.push({

                offset: offset,

                color: color
            });
        }
    };

    module.exports = Gradient;


/***/ }),
/* 22 */
/***/ (function(module, exports, __webpack_require__) {

module.exports = __webpack_require__(23);

/***/ }),
/* 23 */
/***/ (function(module, exports, __webpack_require__) {

var echarts = __webpack_require__(7);
var layoutUtil = __webpack_require__(24);

__webpack_require__(25);
__webpack_require__(72);

var wordCloudLayoutHelper = __webpack_require__(73);

if (!wordCloudLayoutHelper.isSupported) {
    throw new Error('Sorry your browser not support wordCloud');
}

// https://github.com/timdream/wordcloud2.js/blob/c236bee60436e048949f9becc4f0f67bd832dc5c/index.js#L233
function updateCanvasMask(maskCanvas) {
    var ctx = maskCanvas.getContext('2d');
    var imageData = ctx.getImageData(
        0, 0, maskCanvas.width, maskCanvas.height);
    var newImageData = ctx.createImageData(imageData);

    var toneSum = 0;
    var toneCnt = 0;
    for (var i = 0; i < imageData.data.length; i += 4) {
        var alpha = imageData.data[i + 3];
        if (alpha > 128) {
            var tone = imageData.data[i]
                + imageData.data[i + 1]
                + imageData.data[i + 2];
            toneSum += tone;
            ++toneCnt;
        }
    }
    var threshold = toneSum / toneCnt;

    for (var i = 0; i < imageData.data.length; i += 4) {
        var tone = imageData.data[i]
            + imageData.data[i + 1]
            + imageData.data[i + 2];
        var alpha = imageData.data[i + 3];

        if (alpha < 128 || tone > threshold) {
            // Area not to draw
            newImageData.data[i] = 0;
            newImageData.data[i + 1] = 0;
            newImageData.data[i + 2] = 0;
            newImageData.data[i + 3] = 0;
        }
        else {
            // Area to draw
            // The color must be same with backgroundColor
            newImageData.data[i] = 255;
            newImageData.data[i + 1] = 255;
            newImageData.data[i + 2] = 255;
            newImageData.data[i + 3] = 255;
        }
    }

    ctx.putImageData(newImageData, 0, 0);
    console.log(maskCanvas.toDataURL());
}

echarts.registerLayout(function (ecModel, api) {
    ecModel.eachSeriesByType('wordCloud', function (seriesModel) {
        var gridRect = layoutUtil.getLayoutRect(
            seriesModel.getBoxLayoutParams(), {
                width: api.getWidth(),
                height: api.getHeight()
            }
        );
        var data = seriesModel.getData();

        var canvas = document.createElement('canvas');
        canvas.width = gridRect.width;
        canvas.height = gridRect.height;

        var ctx = canvas.getContext('2d');
        var maskImage = seriesModel.get('maskImage');
        if (maskImage) {
            try {
                ctx.drawImage(maskImage, 0, 0, canvas.width, canvas.height);
                updateCanvasMask(canvas);
            }
            catch (e) {
                console.error('Invalid mask image');
                console.error(e.toString());
            }
        }

        var sizeRange = seriesModel.get('sizeRange');
        var rotationRange = seriesModel.get('rotationRange');
        var valueExtent = data.getDataExtent('value');

        var DEGREE_TO_RAD = Math.PI / 180;
        var gridSize = seriesModel.get('gridSize');
        wordCloudLayoutHelper(canvas, {
            list: data.mapArray('value', function (value, idx) {
                var itemModel = data.getItemModel(idx);
                return [
                    data.getName(idx),
                    itemModel.get('textStyle.normal.textSize', true)
                        || echarts.number.linearMap(value, valueExtent, sizeRange),
                    idx
                ];
            }).sort(function (a, b) {
                // Sort from large to small in case there is no more room for more words
                return b[1] - a[1];
            }),
            fontFamily: seriesModel.get('textStyle.normal.fontFamily')
                || seriesModel.get('textStyle.emphasis.fontFamily')
                || ecModel.get('textStyle.fontFamily'),
            fontWeight: seriesModel.get('textStyle.normal.fontWeight')
                || seriesModel.get('textStyle.emphasis.fontWeight')
                || ecModel.get('textStyle.fontWeight'),
            gridSize: gridSize,

            ellipticity: gridRect.height / gridRect.width,

            minRotation: rotationRange[0] * DEGREE_TO_RAD,
            maxRotation: rotationRange[1] * DEGREE_TO_RAD,

            clearCanvas: !maskImage,

            rotateRatio: 1,

            rotationStep: seriesModel.get('rotationStep') * DEGREE_TO_RAD,

            drawOutOfBound: seriesModel.get('drawOutOfBound'),

            shuffle: false,

            shape: seriesModel.get('shape')
        });

        function onWordCloudDrawn(e) {
            var item = e.detail.item;
            if (e.detail.drawn && seriesModel.layoutInstance.ondraw) {
                e.detail.drawn.gx += gridRect.x / gridSize;
                e.detail.drawn.gy += gridRect.y / gridSize;
                seriesModel.layoutInstance.ondraw(
                    item[0], item[1], item[2], e.detail.drawn
                );
            }
        }

        canvas.addEventListener('wordclouddrawn', onWordCloudDrawn);

        if (seriesModel.layoutInstance) {
            // Dispose previous
            seriesModel.layoutInstance.dispose();
        }

        seriesModel.layoutInstance = {
            ondraw: null,

            dispose: function () {
                canvas.removeEventListener('wordclouddrawn', onWordCloudDrawn);
                // Abort
                canvas.addEventListener('wordclouddrawn', function (e) {
                    // Prevent default to cancle the event and stop the loop
                    e.preventDefault();
                });
            }
        };
    });
});


/***/ }),
/* 24 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

// Layout helpers for each component positioning


    var zrUtil = __webpack_require__(0);
    var BoundingRect = __webpack_require__(3);
    var numberUtil = __webpack_require__(9);
    var formatUtil = __webpack_require__(12);
    var parsePercent = numberUtil.parsePercent;
    var each = zrUtil.each;

    var layout = {};

    /**
     * @public
     */
    var LOCATION_PARAMS = layout.LOCATION_PARAMS = [
        'left', 'right', 'top', 'bottom', 'width', 'height'
    ];

    /**
     * @public
     */
    var HV_NAMES = layout.HV_NAMES = [
        ['width', 'left', 'right'],
        ['height', 'top', 'bottom']
    ];

    function boxLayout(orient, group, gap, maxWidth, maxHeight) {
        var x = 0;
        var y = 0;
        if (maxWidth == null) {
            maxWidth = Infinity;
        }
        if (maxHeight == null) {
            maxHeight = Infinity;
        }
        var currentLineMaxSize = 0;
        group.eachChild(function (child, idx) {
            var position = child.position;
            var rect = child.getBoundingRect();
            var nextChild = group.childAt(idx + 1);
            var nextChildRect = nextChild && nextChild.getBoundingRect();
            var nextX;
            var nextY;
            if (orient === 'horizontal') {
                var moveX = rect.width + (nextChildRect ? (-nextChildRect.x + rect.x) : 0);
                nextX = x + moveX;
                // Wrap when width exceeds maxWidth or meet a `newline` group
                if (nextX > maxWidth || child.newline) {
                    x = 0;
                    nextX = moveX;
                    y += currentLineMaxSize + gap;
                    currentLineMaxSize = rect.height;
                }
                else {
                    currentLineMaxSize = Math.max(currentLineMaxSize, rect.height);
                }
            }
            else {
                var moveY = rect.height + (nextChildRect ? (-nextChildRect.y + rect.y) : 0);
                nextY = y + moveY;
                // Wrap when width exceeds maxHeight or meet a `newline` group
                if (nextY > maxHeight || child.newline) {
                    x += currentLineMaxSize + gap;
                    y = 0;
                    nextY = moveY;
                    currentLineMaxSize = rect.width;
                }
                else {
                    currentLineMaxSize = Math.max(currentLineMaxSize, rect.width);
                }
            }

            if (child.newline) {
                return;
            }

            position[0] = x;
            position[1] = y;

            orient === 'horizontal'
                ? (x = nextX + gap)
                : (y = nextY + gap);
        });
    }

    /**
     * VBox or HBox layouting
     * @param {string} orient
     * @param {module:zrender/container/Group} group
     * @param {number} gap
     * @param {number} [width=Infinity]
     * @param {number} [height=Infinity]
     */
    layout.box = boxLayout;

    /**
     * VBox layouting
     * @param {module:zrender/container/Group} group
     * @param {number} gap
     * @param {number} [width=Infinity]
     * @param {number} [height=Infinity]
     */
    layout.vbox = zrUtil.curry(boxLayout, 'vertical');

    /**
     * HBox layouting
     * @param {module:zrender/container/Group} group
     * @param {number} gap
     * @param {number} [width=Infinity]
     * @param {number} [height=Infinity]
     */
    layout.hbox = zrUtil.curry(boxLayout, 'horizontal');

    /**
     * If x or x2 is not specified or 'center' 'left' 'right',
     * the width would be as long as possible.
     * If y or y2 is not specified or 'middle' 'top' 'bottom',
     * the height would be as long as possible.
     *
     * @param {Object} positionInfo
     * @param {number|string} [positionInfo.x]
     * @param {number|string} [positionInfo.y]
     * @param {number|string} [positionInfo.x2]
     * @param {number|string} [positionInfo.y2]
     * @param {Object} containerRect
     * @param {string|number} margin
     * @return {Object} {width, height}
     */
    layout.getAvailableSize = function (positionInfo, containerRect, margin) {
        var containerWidth = containerRect.width;
        var containerHeight = containerRect.height;

        var x = parsePercent(positionInfo.x, containerWidth);
        var y = parsePercent(positionInfo.y, containerHeight);
        var x2 = parsePercent(positionInfo.x2, containerWidth);
        var y2 = parsePercent(positionInfo.y2, containerHeight);

        (isNaN(x) || isNaN(parseFloat(positionInfo.x))) && (x = 0);
        (isNaN(x2) || isNaN(parseFloat(positionInfo.x2))) && (x2 = containerWidth);
        (isNaN(y) || isNaN(parseFloat(positionInfo.y))) && (y = 0);
        (isNaN(y2) || isNaN(parseFloat(positionInfo.y2))) && (y2 = containerHeight);

        margin = formatUtil.normalizeCssArray(margin || 0);

        return {
            width: Math.max(x2 - x - margin[1] - margin[3], 0),
            height: Math.max(y2 - y - margin[0] - margin[2], 0)
        };
    };

    /**
     * Parse position info.
     *
     * @param {Object} positionInfo
     * @param {number|string} [positionInfo.left]
     * @param {number|string} [positionInfo.top]
     * @param {number|string} [positionInfo.right]
     * @param {number|string} [positionInfo.bottom]
     * @param {number|string} [positionInfo.width]
     * @param {number|string} [positionInfo.height]
     * @param {number|string} [positionInfo.aspect] Aspect is width / height
     * @param {Object} containerRect
     * @param {string|number} [margin]
     *
     * @return {module:zrender/core/BoundingRect}
     */
    layout.getLayoutRect = function (
        positionInfo, containerRect, margin
    ) {
        margin = formatUtil.normalizeCssArray(margin || 0);

        var containerWidth = containerRect.width;
        var containerHeight = containerRect.height;

        var left = parsePercent(positionInfo.left, containerWidth);
        var top = parsePercent(positionInfo.top, containerHeight);
        var right = parsePercent(positionInfo.right, containerWidth);
        var bottom = parsePercent(positionInfo.bottom, containerHeight);
        var width = parsePercent(positionInfo.width, containerWidth);
        var height = parsePercent(positionInfo.height, containerHeight);

        var verticalMargin = margin[2] + margin[0];
        var horizontalMargin = margin[1] + margin[3];
        var aspect = positionInfo.aspect;

        // If width is not specified, calculate width from left and right
        if (isNaN(width)) {
            width = containerWidth - right - horizontalMargin - left;
        }
        if (isNaN(height)) {
            height = containerHeight - bottom - verticalMargin - top;
        }

        // If width and height are not given
        // 1. Graph should not exceeds the container
        // 2. Aspect must be keeped
        // 3. Graph should take the space as more as possible
        if (isNaN(width) && isNaN(height)) {
            if (aspect > containerWidth / containerHeight) {
                width = containerWidth * 0.8;
            }
            else {
                height = containerHeight * 0.8;
            }
        }

        if (aspect != null) {
            // Calculate width or height with given aspect
            if (isNaN(width)) {
                width = aspect * height;
            }
            if (isNaN(height)) {
                height = width / aspect;
            }
        }

        // If left is not specified, calculate left from right and width
        if (isNaN(left)) {
            left = containerWidth - right - width - horizontalMargin;
        }
        if (isNaN(top)) {
            top = containerHeight - bottom - height - verticalMargin;
        }

        // Align left and top
        switch (positionInfo.left || positionInfo.right) {
            case 'center':
                left = containerWidth / 2 - width / 2 - margin[3];
                break;
            case 'right':
                left = containerWidth - width - horizontalMargin;
                break;
        }
        switch (positionInfo.top || positionInfo.bottom) {
            case 'middle':
            case 'center':
                top = containerHeight / 2 - height / 2 - margin[0];
                break;
            case 'bottom':
                top = containerHeight - height - verticalMargin;
                break;
        }
        // If something is wrong and left, top, width, height are calculated as NaN
        left = left || 0;
        top = top || 0;
        if (isNaN(width)) {
            // Width may be NaN if only one value is given except width
            width = containerWidth - left - (right || 0);
        }
        if (isNaN(height)) {
            // Height may be NaN if only one value is given except height
            height = containerHeight - top - (bottom || 0);
        }

        var rect = new BoundingRect(left + margin[3], top + margin[0], width, height);
        rect.margin = margin;
        return rect;
    };


    /**
     * Position a zr element in viewport
     *  Group position is specified by either
     *  {left, top}, {right, bottom}
     *  If all properties exists, right and bottom will be igonred.
     *
     * Logic:
     *     1. Scale (against origin point in parent coord)
     *     2. Rotate (against origin point in parent coord)
     *     3. Traslate (with el.position by this method)
     * So this method only fixes the last step 'Traslate', which does not affect
     * scaling and rotating.
     *
     * If be called repeatly with the same input el, the same result will be gotten.
     *
     * @param {module:zrender/Element} el Should have `getBoundingRect` method.
     * @param {Object} positionInfo
     * @param {number|string} [positionInfo.left]
     * @param {number|string} [positionInfo.top]
     * @param {number|string} [positionInfo.right]
     * @param {number|string} [positionInfo.bottom]
     * @param {Object} containerRect
     * @param {string|number} margin
     * @param {Object} [opt]
     * @param {Array.<number>} [opt.hv=[1,1]] Only horizontal or only vertical.
     * @param {Array.<number>} [opt.boundingMode='all']
     *        Specify how to calculate boundingRect when locating.
     *        'all': Position the boundingRect that is transformed and uioned
     *               both itself and its descendants.
     *               This mode simplies confine the elements in the bounding
     *               of their container (e.g., using 'right: 0').
     *        'raw': Position the boundingRect that is not transformed and only itself.
     *               This mode is useful when you want a element can overflow its
     *               container. (Consider a rotated circle needs to be located in a corner.)
     *               In this mode positionInfo.width/height can only be number.
     */
    layout.positionElement = function (el, positionInfo, containerRect, margin, opt) {
        var h = !opt || !opt.hv || opt.hv[0];
        var v = !opt || !opt.hv || opt.hv[1];
        var boundingMode = opt && opt.boundingMode || 'all';

        if (!h && !v) {
            return;
        }

        var rect;
        if (boundingMode === 'raw') {
            rect = el.type === 'group'
                ? new BoundingRect(0, 0, +positionInfo.width || 0, +positionInfo.height || 0)
                : el.getBoundingRect();
        }
        else {
            rect = el.getBoundingRect();
            if (el.needLocalTransform()) {
                var transform = el.getLocalTransform();
                // Notice: raw rect may be inner object of el,
                // which should not be modified.
                rect = rect.clone();
                rect.applyTransform(transform);
            }
        }

        positionInfo = layout.getLayoutRect(
            zrUtil.defaults(
                {width: rect.width, height: rect.height},
                positionInfo
            ),
            containerRect,
            margin
        );

        // Because 'tranlate' is the last step in transform
        // (see zrender/core/Transformable#getLocalTransfrom),
        // we can just only modify el.position to get final result.
        var elPos = el.position;
        var dx = h ? positionInfo.x - rect.x : 0;
        var dy = v ? positionInfo.y - rect.y : 0;

        el.attr('position', boundingMode === 'raw' ? [dx, dy] : [elPos[0] + dx, elPos[1] + dy]);
    };

    /**
     * @param {Object} option Contains some of the properties in HV_NAMES.
     * @param {number} hvIdx 0: horizontal; 1: vertical.
     */
    layout.sizeCalculable = function (option, hvIdx) {
        return option[HV_NAMES[hvIdx][0]] != null
            || (option[HV_NAMES[hvIdx][1]] != null && option[HV_NAMES[hvIdx][2]] != null);
    };

    /**
     * Consider Case:
     * When defulat option has {left: 0, width: 100}, and we set {right: 0}
     * through setOption or media query, using normal zrUtil.merge will cause
     * {right: 0} does not take effect.
     *
     * @example
     * ComponentModel.extend({
     *     init: function () {
     *         ...
     *         var inputPositionParams = layout.getLayoutParams(option);
     *         this.mergeOption(inputPositionParams);
     *     },
     *     mergeOption: function (newOption) {
     *         newOption && zrUtil.merge(thisOption, newOption, true);
     *         layout.mergeLayoutParam(thisOption, newOption);
     *     }
     * });
     *
     * @param {Object} targetOption
     * @param {Object} newOption
     * @param {Object|string} [opt]
     * @param {boolean|Array.<boolean>} [opt.ignoreSize=false] Some component must has width and height.
     */
    layout.mergeLayoutParam = function (targetOption, newOption, opt) {
        !zrUtil.isObject(opt) && (opt = {});

        var ignoreSize = opt.ignoreSize;
        !zrUtil.isArray(ignoreSize) && (ignoreSize = [ignoreSize, ignoreSize]);

        var hResult = merge(HV_NAMES[0], 0);
        var vResult = merge(HV_NAMES[1], 1);

        copy(HV_NAMES[0], targetOption, hResult);
        copy(HV_NAMES[1], targetOption, vResult);

        function merge(names, hvIdx) {
            var newParams = {};
            var newValueCount = 0;
            var merged = {};
            var mergedValueCount = 0;
            var enoughParamNumber = 2;

            each(names, function (name) {
                merged[name] = targetOption[name];
            });
            each(names, function (name) {
                // Consider case: newOption.width is null, which is
                // set by user for removing width setting.
                hasProp(newOption, name) && (newParams[name] = merged[name] = newOption[name]);
                hasValue(newParams, name) && newValueCount++;
                hasValue(merged, name) && mergedValueCount++;
            });

            if (ignoreSize[hvIdx]) {
                // Only one of left/right is premitted to exist.
                if (hasValue(newOption, names[1])) {
                    merged[names[2]] = null;
                }
                else if (hasValue(newOption, names[2])) {
                    merged[names[1]] = null;
                }
                return merged;
            }

            // Case: newOption: {width: ..., right: ...},
            // or targetOption: {right: ...} and newOption: {width: ...},
            // There is no conflict when merged only has params count
            // little than enoughParamNumber.
            if (mergedValueCount === enoughParamNumber || !newValueCount) {
                return merged;
            }
            // Case: newOption: {width: ..., right: ...},
            // Than we can make sure user only want those two, and ignore
            // all origin params in targetOption.
            else if (newValueCount >= enoughParamNumber) {
                return newParams;
            }
            else {
                // Chose another param from targetOption by priority.
                for (var i = 0; i < names.length; i++) {
                    var name = names[i];
                    if (!hasProp(newParams, name) && hasProp(targetOption, name)) {
                        newParams[name] = targetOption[name];
                        break;
                    }
                }
                return newParams;
            }
        }

        function hasProp(obj, name) {
            return obj.hasOwnProperty(name);
        }

        function hasValue(obj, name) {
            return obj[name] != null && obj[name] !== 'auto';
        }

        function copy(names, target, source) {
            each(names, function (name) {
                target[name] = source[name];
            });
        }
    };

    /**
     * Retrieve 'left', 'right', 'top', 'bottom', 'width', 'height' from object.
     * @param {Object} source
     * @return {Object} Result contains those props.
     */
    layout.getLayoutParams = function (source) {
        return layout.copyLayoutParams({}, source);
    };

    /**
     * Retrieve 'left', 'right', 'top', 'bottom', 'width', 'height' from object.
     * @param {Object} source
     * @return {Object} Result contains those props.
     */
    layout.copyLayoutParams = function (target, source) {
        source && target && each(LOCATION_PARAMS, function (name) {
            source.hasOwnProperty(name) && (target[name] = source[name]);
        });
        return target;
    };

    module.exports = layout;



/***/ }),
/* 25 */
/***/ (function(module, exports, __webpack_require__) {

var completeDimensions = __webpack_require__(26);
var echarts = __webpack_require__(7);

echarts.extendSeriesModel({

    type: 'series.wordCloud',

    visualColorAccessPath: 'textStyle.normal.color',

    optionUpdated: function () {
        var option = this.option;
        option.gridSize = Math.max(Math.floor(option.gridSize), 4);
    },

    getInitialData: function (option, ecModel) {
        var dimensions = completeDimensions(['value'], option.data);
        var list = new echarts.List(dimensions, this);
        list.initData(option.data);
        return list;
    },

    // Most of options are from https://github.com/timdream/wordcloud2.js/blob/gh-pages/API.md
    defaultOption: {

        maskImage: null,

        // Shape can be 'circle', 'cardioid', 'diamond', 'triangle-forward', 'triangle', 'pentagon', 'star'
        shape: 'circle',

        left: 'center',

        top: 'center',

        width: '70%',

        height: '80%',

        sizeRange: [12, 60],

        rotationRange: [-90, 90],

        rotationStep: 45,

        gridSize: 8,

        drawOutOfBound: false,

        textStyle: {
            normal: {
                fontWeight: 'normal'
            }
        }
    }
});

/***/ }),
/* 26 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Complete dimensions by data (guess dimension).
 */


    var zrUtil = __webpack_require__(0);
    var modelUtil = __webpack_require__(27);
    var each = zrUtil.each;
    var isString = zrUtil.isString;
    var defaults = zrUtil.defaults;
    var normalizeToArray = modelUtil.normalizeToArray;

    var OTHER_DIMS = {tooltip: 1, label: 1, itemName: 1};

    /**
     * Complete the dimensions array, by user defined `dimension` and `encode`,
     * and guessing from the data structure.
     * If no 'value' dimension specified, the first no-named dimension will be
     * named as 'value'.
     *
     * @param {Array.<string>} sysDims Necessary dimensions, like ['x', 'y'], which
     *      provides not only dim template, but also default order.
     *      `name` of each item provides default coord name.
     *      [{dimsDef: []}, ...] can be specified to give names.
     * @param {Array} data Data list. [[1, 2, 3], [2, 3, 4]].
     * @param {Object} [opt]
     * @param {Array.<Object|string>} [opt.dimsDef] option.series.dimensions User defined dimensions
     *      For example: ['asdf', {name, type}, ...].
     * @param {Object} [opt.encodeDef] option.series.encode {x: 2, y: [3, 1], tooltip: [1, 2], label: 3}
     * @param {string} [opt.extraPrefix] Prefix of name when filling the left dimensions.
     * @param {string} [opt.extraFromZero] If specified, extra dim names will be:
     *                      extraPrefix + 0, extraPrefix + extraBaseIndex + 1 ...
     *                      If not specified, extra dim names will be:
     *                      extraPrefix, extraPrefix + 0, extraPrefix + 1 ...
     * @param {number} [opt.dimCount] If not specified, guess by the first data item.
     * @return {Array.<Object>} [{
     *      name: string mandatory,
     *      coordDim: string mandatory,
     *      coordDimIndex: number mandatory,
     *      type: string optional,
     *      tooltipName: string optional,
     *      otherDims: {
     *          tooltip: number optional,
     *          label: number optional
     *      },
     *      isExtraCoord: boolean true or undefined.
     *      other props ...
     * }]
     */
    function completeDimensions(sysDims, data, opt) {
        data = data || [];
        opt = opt || {};
        sysDims = (sysDims || []).slice();
        var dimsDef = (opt.dimsDef || []).slice();
        var encodeDef = zrUtil.createHashMap(opt.encodeDef);
        var dataDimNameMap = zrUtil.createHashMap();
        var coordDimNameMap = zrUtil.createHashMap();
        // var valueCandidate;
        var result = [];

        var dimCount = opt.dimCount;
        if (dimCount == null) {
            var value0 = retrieveValue(data[0]);
            dimCount = Math.max(
                zrUtil.isArray(value0) && value0.length || 1,
                sysDims.length,
                dimsDef.length
            );
            each(sysDims, function (sysDimItem) {
                var sysDimItemDimsDef = sysDimItem.dimsDef;
                sysDimItemDimsDef && (dimCount = Math.max(dimCount, sysDimItemDimsDef.length));
            });
        }

        // Apply user defined dims (`name` and `type`) and init result.
        for (var i = 0; i < dimCount; i++) {
            var dimDefItem = isString(dimsDef[i]) ? {name: dimsDef[i]} : (dimsDef[i] || {});
            var userDimName = dimDefItem.name;
            var resultItem = result[i] = {otherDims: {}};
            // Name will be applied later for avoiding duplication.
            if (userDimName != null && dataDimNameMap.get(userDimName) == null) {
                // Only if `series.dimensions` is defined in option, tooltipName
                // will be set, and dimension will be diplayed vertically in
                // tooltip by default.
                resultItem.name = resultItem.tooltipName = userDimName;
                dataDimNameMap.set(userDimName, i);
            }
            dimDefItem.type != null && (resultItem.type = dimDefItem.type);
        }

        // Set `coordDim` and `coordDimIndex` by `encodeDef` and normalize `encodeDef`.
        encodeDef.each(function (dataDims, coordDim) {
            dataDims = encodeDef.set(coordDim, normalizeToArray(dataDims).slice());
            each(dataDims, function (resultDimIdx, coordDimIndex) {
                // The input resultDimIdx can be dim name or index.
                isString(resultDimIdx) && (resultDimIdx = dataDimNameMap.get(resultDimIdx));
                if (resultDimIdx != null && resultDimIdx < dimCount) {
                    dataDims[coordDimIndex] = resultDimIdx;
                    applyDim(result[resultDimIdx], coordDim, coordDimIndex);
                }
            });
        });

        // Apply templetes and default order from `sysDims`.
        var availDimIdx = 0;
        each(sysDims, function (sysDimItem, sysDimIndex) {
            var coordDim;
            var sysDimItem;
            var sysDimItemDimsDef;
            var sysDimItemOtherDims;
            if (isString(sysDimItem)) {
                coordDim = sysDimItem;
                sysDimItem = {};
            }
            else {
                coordDim = sysDimItem.name;
                sysDimItem = zrUtil.clone(sysDimItem);
                // `coordDimIndex` should not be set directly.
                sysDimItemDimsDef = sysDimItem.dimsDef;
                sysDimItemOtherDims = sysDimItem.otherDims;
                sysDimItem.name = sysDimItem.coordDim = sysDimItem.coordDimIndex
                    = sysDimItem.dimsDef = sysDimItem.otherDims = null;
            }

            var dataDims = normalizeToArray(encodeDef.get(coordDim));
            // dimensions provides default dim sequences.
            if (!dataDims.length) {
                for (var i = 0; i < (sysDimItemDimsDef && sysDimItemDimsDef.length || 1); i++) {
                    while (availDimIdx < result.length && result[availDimIdx].coordDim != null) {
                        availDimIdx++;
                    }
                    availDimIdx < result.length && dataDims.push(availDimIdx++);
                }
            }
            // Apply templates.
            each(dataDims, function (resultDimIdx, coordDimIndex) {
                var resultItem = result[resultDimIdx];
                applyDim(defaults(resultItem, sysDimItem), coordDim, coordDimIndex);
                if (resultItem.name == null && sysDimItemDimsDef) {
                    resultItem.name = resultItem.tooltipName = sysDimItemDimsDef[coordDimIndex];
                }
                sysDimItemOtherDims && defaults(resultItem.otherDims, sysDimItemOtherDims);
            });
        });

        // Make sure the first extra dim is 'value'.
        var extra = opt.extraPrefix || 'value';

        // Set dim `name` and other `coordDim` and other props.
        for (var resultDimIdx = 0; resultDimIdx < dimCount; resultDimIdx++) {
            var resultItem = result[resultDimIdx] = result[resultDimIdx] || {};
            var coordDim = resultItem.coordDim;

            coordDim == null && (
                resultItem.coordDim = genName(extra, coordDimNameMap, opt.extraFromZero),
                resultItem.coordDimIndex = 0,
                resultItem.isExtraCoord = true
            );

            resultItem.name == null && (resultItem.name = genName(
                resultItem.coordDim,
                dataDimNameMap
            ));

            resultItem.type == null && guessOrdinal(data, resultDimIdx)
                && (resultItem.type = 'ordinal');
        }

        return result;

        function applyDim(resultItem, coordDim, coordDimIndex) {
            if (OTHER_DIMS[coordDim]) {
                resultItem.otherDims[coordDim] = coordDimIndex;
            }
            else {
                resultItem.coordDim = coordDim;
                resultItem.coordDimIndex = coordDimIndex;
                coordDimNameMap.set(coordDim, true);
            }
        }

        function genName(name, map, fromZero) {
            if (fromZero || map.get(name) != null) {
                var i = 0;
                while (map.get(name + i) != null) {
                    i++;
                }
                name += i;
            }
            map.set(name, true);
            return name;
        }
    }

    // The rule should not be complex, otherwise user might not
    // be able to known where the data is wrong.
    var guessOrdinal = completeDimensions.guessOrdinal = function (data, dimIndex) {
        for (var i = 0, len = data.length; i < len; i++) {
            var value = retrieveValue(data[i]);

            if (!zrUtil.isArray(value)) {
                return false;
            }

            var value = value[dimIndex];
            // Consider usage convenience, '1', '2' will be treated as "number".
            if (value != null && isFinite(value)) {
                return false;
            }
            else if (isString(value) && value !== '-') {
                return true;
            }
        }
        return false;
    };

    function retrieveValue(o) {
        return zrUtil.isArray(o) ? o : zrUtil.isObject(o) ? o.value: o;
    }

    module.exports = completeDimensions;



/***/ }),
/* 27 */
/***/ (function(module, exports, __webpack_require__) {



    var formatUtil = __webpack_require__(12);
    var nubmerUtil = __webpack_require__(9);
    var Model = __webpack_require__(28);
    var zrUtil = __webpack_require__(0);
    var each = zrUtil.each;
    var isObject = zrUtil.isObject;

    var modelUtil = {};

    /**
     * If value is not array, then translate it to array.
     * @param  {*} value
     * @return {Array} [value] or value
     */
    modelUtil.normalizeToArray = function (value) {
        return value instanceof Array
            ? value
            : value == null
            ? []
            : [value];
    };

    /**
     * Sync default option between normal and emphasis like `position` and `show`
     * In case some one will write code like
     *     label: {
     *         normal: {
     *             show: false,
     *             position: 'outside',
     *             textStyle: {
     *                 fontSize: 18
     *             }
     *         },
     *         emphasis: {
     *             show: true
     *         }
     *     }
     * @param {Object} opt
     * @param {Array.<string>} subOpts
     */
     modelUtil.defaultEmphasis = function (opt, subOpts) {
        if (opt) {
            var emphasisOpt = opt.emphasis = opt.emphasis || {};
            var normalOpt = opt.normal = opt.normal || {};

            // Default emphasis option from normal
            each(subOpts, function (subOptName) {
                var val = zrUtil.retrieve(emphasisOpt[subOptName], normalOpt[subOptName]);
                if (val != null) {
                    emphasisOpt[subOptName] = val;
                }
            });
        }
    };

    modelUtil.LABEL_OPTIONS = ['position', 'offset', 'show', 'textStyle', 'distance', 'formatter'];

    /**
     * data could be [12, 2323, {value: 223}, [1221, 23], {value: [2, 23]}]
     * This helper method retieves value from data.
     * @param {string|number|Date|Array|Object} dataItem
     * @return {number|string|Date|Array.<number|string|Date>}
     */
    modelUtil.getDataItemValue = function (dataItem) {
        // Performance sensitive.
        return dataItem && (dataItem.value == null ? dataItem : dataItem.value);
    };

    /**
     * data could be [12, 2323, {value: 223}, [1221, 23], {value: [2, 23]}]
     * This helper method determine if dataItem has extra option besides value
     * @param {string|number|Date|Array|Object} dataItem
     */
    modelUtil.isDataItemOption = function (dataItem) {
        return isObject(dataItem)
            && !(dataItem instanceof Array);
            // // markLine data can be array
            // && !(dataItem[0] && isObject(dataItem[0]) && !(dataItem[0] instanceof Array));
    };

    /**
     * This helper method convert value in data.
     * @param {string|number|Date} value
     * @param {Object|string} [dimInfo] If string (like 'x'), dimType defaults 'number'.
     */
    modelUtil.converDataValue = function (value, dimInfo) {
        // Performance sensitive.
        var dimType = dimInfo && dimInfo.type;
        if (dimType === 'ordinal') {
            return value;
        }

        if (dimType === 'time'
            // spead up when using timestamp
            && typeof value !== 'number'
            && value != null
            && value !== '-'
        ) {
            value = +nubmerUtil.parseDate(value);
        }

        // dimType defaults 'number'.
        // If dimType is not ordinal and value is null or undefined or NaN or '-',
        // parse to NaN.
        return (value == null || value === '')
            ? NaN : +value; // If string (like '-'), using '+' parse to NaN
    };

    /**
     * Create a model proxy to be used in tooltip for edge data, markLine data, markPoint data.
     * @param {module:echarts/data/List} data
     * @param {Object} opt
     * @param {string} [opt.seriesIndex]
     * @param {Object} [opt.name]
     * @param {Object} [opt.mainType]
     * @param {Object} [opt.subType]
     */
    modelUtil.createDataFormatModel = function (data, opt) {
        var model = new Model();
        zrUtil.mixin(model, modelUtil.dataFormatMixin);
        model.seriesIndex = opt.seriesIndex;
        model.name = opt.name || '';
        model.mainType = opt.mainType;
        model.subType = opt.subType;

        model.getData = function () {
            return data;
        };
        return model;
    };

    // PENDING A little ugly
    modelUtil.dataFormatMixin = {
        /**
         * Get params for formatter
         * @param {number} dataIndex
         * @param {string} [dataType]
         * @return {Object}
         */
        getDataParams: function (dataIndex, dataType) {
            var data = this.getData(dataType);
            var rawValue = this.getRawValue(dataIndex, dataType);
            var rawDataIndex = data.getRawIndex(dataIndex);
            var name = data.getName(dataIndex, true);
            var itemOpt = data.getRawDataItem(dataIndex);
            var color = data.getItemVisual(dataIndex, 'color');

            return {
                componentType: this.mainType,
                componentSubType: this.subType,
                seriesType: this.mainType === 'series' ? this.subType : null,
                seriesIndex: this.seriesIndex,
                seriesId: this.id,
                seriesName: this.name,
                name: name,
                dataIndex: rawDataIndex,
                data: itemOpt,
                dataType: dataType,
                value: rawValue,
                color: color,
                marker: formatUtil.getTooltipMarker(color),

                // Param name list for mapping `a`, `b`, `c`, `d`, `e`
                $vars: ['seriesName', 'name', 'value']
            };
        },

        /**
         * Format label
         * @param {number} dataIndex
         * @param {string} [status='normal'] 'normal' or 'emphasis'
         * @param {string} [dataType]
         * @param {number} [dimIndex]
         * @param {string} [labelProp='label']
         * @return {string}
         */
        getFormattedLabel: function (dataIndex, status, dataType, dimIndex, labelProp) {
            status = status || 'normal';
            var data = this.getData(dataType);
            var itemModel = data.getItemModel(dataIndex);

            var params = this.getDataParams(dataIndex, dataType);
            if (dimIndex != null && (params.value instanceof Array)) {
                params.value = params.value[dimIndex];
            }

            var formatter = itemModel.get([labelProp || 'label', status, 'formatter']);

            if (typeof formatter === 'function') {
                params.status = status;
                return formatter(params);
            }
            else if (typeof formatter === 'string') {
                return formatUtil.formatTpl(formatter, params);
            }
        },

        /**
         * Get raw value in option
         * @param {number} idx
         * @param {string} [dataType]
         * @return {Object}
         */
        getRawValue: function (idx, dataType) {
            var data = this.getData(dataType);
            var dataItem = data.getRawDataItem(idx);
            if (dataItem != null) {
                return (isObject(dataItem) && !(dataItem instanceof Array))
                    ? dataItem.value : dataItem;
            }
        },

        /**
         * Should be implemented.
         * @param {number} dataIndex
         * @param {boolean} [multipleSeries=false]
         * @param {number} [dataType]
         * @return {string} tooltip string
         */
        formatTooltip: zrUtil.noop
    };

    /**
     * Mapping to exists for merge.
     *
     * @public
     * @param {Array.<Object>|Array.<module:echarts/model/Component>} exists
     * @param {Object|Array.<Object>} newCptOptions
     * @return {Array.<Object>} Result, like [{exist: ..., option: ...}, {}],
     *                          index of which is the same as exists.
     */
    modelUtil.mappingToExists = function (exists, newCptOptions) {
        // Mapping by the order by original option (but not order of
        // new option) in merge mode. Because we should ensure
        // some specified index (like xAxisIndex) is consistent with
        // original option, which is easy to understand, espatially in
        // media query. And in most case, merge option is used to
        // update partial option but not be expected to change order.
        newCptOptions = (newCptOptions || []).slice();

        var result = zrUtil.map(exists || [], function (obj, index) {
            return {exist: obj};
        });

        // Mapping by id or name if specified.
        each(newCptOptions, function (cptOption, index) {
            if (!isObject(cptOption)) {
                return;
            }

            // id has highest priority.
            for (var i = 0; i < result.length; i++) {
                if (!result[i].option // Consider name: two map to one.
                    && cptOption.id != null
                    && result[i].exist.id === cptOption.id + ''
                ) {
                    result[i].option = cptOption;
                    newCptOptions[index] = null;
                    return;
                }
            }

            for (var i = 0; i < result.length; i++) {
                var exist = result[i].exist;
                if (!result[i].option // Consider name: two map to one.
                    // Can not match when both ids exist but different.
                    && (exist.id == null || cptOption.id == null)
                    && cptOption.name != null
                    && !modelUtil.isIdInner(cptOption)
                    && !modelUtil.isIdInner(exist)
                    && exist.name === cptOption.name + ''
                ) {
                    result[i].option = cptOption;
                    newCptOptions[index] = null;
                    return;
                }
            }
        });

        // Otherwise mapping by index.
        each(newCptOptions, function (cptOption, index) {
            if (!isObject(cptOption)) {
                return;
            }

            var i = 0;
            for (; i < result.length; i++) {
                var exist = result[i].exist;
                if (!result[i].option
                    // Existing model that already has id should be able to
                    // mapped to (because after mapping performed model may
                    // be assigned with a id, whish should not affect next
                    // mapping), except those has inner id.
                    && !modelUtil.isIdInner(exist)
                    // Caution:
                    // Do not overwrite id. But name can be overwritten,
                    // because axis use name as 'show label text'.
                    // 'exist' always has id and name and we dont
                    // need to check it.
                    && cptOption.id == null
                ) {
                    result[i].option = cptOption;
                    break;
                }
            }

            if (i >= result.length) {
                result.push({option: cptOption});
            }
        });

        return result;
    };

    /**
     * Make id and name for mapping result (result of mappingToExists)
     * into `keyInfo` field.
     *
     * @public
     * @param {Array.<Object>} Result, like [{exist: ..., option: ...}, {}],
     *                          which order is the same as exists.
     * @return {Array.<Object>} The input.
     */
    modelUtil.makeIdAndName = function (mapResult) {
        // We use this id to hash component models and view instances
        // in echarts. id can be specified by user, or auto generated.

        // The id generation rule ensures new view instance are able
        // to mapped to old instance when setOption are called in
        // no-merge mode. So we generate model id by name and plus
        // type in view id.

        // name can be duplicated among components, which is convenient
        // to specify multi components (like series) by one name.

        // Ensure that each id is distinct.
        var idMap = zrUtil.createHashMap();

        each(mapResult, function (item, index) {
            var existCpt = item.exist;
            existCpt && idMap.set(existCpt.id, item);
        });

        each(mapResult, function (item, index) {
            var opt = item.option;

            zrUtil.assert(
                !opt || opt.id == null || !idMap.get(opt.id) || idMap.get(opt.id) === item,
                'id duplicates: ' + (opt && opt.id)
            );

            opt && opt.id != null && idMap.set(opt.id, item);
            !item.keyInfo && (item.keyInfo = {});
        });

        // Make name and id.
        each(mapResult, function (item, index) {
            var existCpt = item.exist;
            var opt = item.option;
            var keyInfo = item.keyInfo;

            if (!isObject(opt)) {
                return;
            }

            // name can be overwitten. Consider case: axis.name = '20km'.
            // But id generated by name will not be changed, which affect
            // only in that case: setOption with 'not merge mode' and view
            // instance will be recreated, which can be accepted.
            keyInfo.name = opt.name != null
                ? opt.name + ''
                : existCpt
                ? existCpt.name
                : '\0-'; // name may be displayed on screen, so use '-'.

            if (existCpt) {
                keyInfo.id = existCpt.id;
            }
            else if (opt.id != null) {
                keyInfo.id = opt.id + '';
            }
            else {
                // Consider this situatoin:
                //  optionA: [{name: 'a'}, {name: 'a'}, {..}]
                //  optionB [{..}, {name: 'a'}, {name: 'a'}]
                // Series with the same name between optionA and optionB
                // should be mapped.
                var idNum = 0;
                do {
                    keyInfo.id = '\0' + keyInfo.name + '\0' + idNum++;
                }
                while (idMap.get(keyInfo.id));
            }

            idMap.set(keyInfo.id, item);
        });
    };

    /**
     * @public
     * @param {Object} cptOption
     * @return {boolean}
     */
    modelUtil.isIdInner = function (cptOption) {
        return isObject(cptOption)
            && cptOption.id
            && (cptOption.id + '').indexOf('\0_ec_\0') === 0;
    };

    /**
     * A helper for removing duplicate items between batchA and batchB,
     * and in themselves, and categorize by series.
     *
     * @param {Array.<Object>} batchA Like: [{seriesId: 2, dataIndex: [32, 4, 5]}, ...]
     * @param {Array.<Object>} batchB Like: [{seriesId: 2, dataIndex: [32, 4, 5]}, ...]
     * @return {Array.<Array.<Object>, Array.<Object>>} result: [resultBatchA, resultBatchB]
     */
    modelUtil.compressBatches = function (batchA, batchB) {
        var mapA = {};
        var mapB = {};

        makeMap(batchA || [], mapA);
        makeMap(batchB || [], mapB, mapA);

        return [mapToArray(mapA), mapToArray(mapB)];

        function makeMap(sourceBatch, map, otherMap) {
            for (var i = 0, len = sourceBatch.length; i < len; i++) {
                var seriesId = sourceBatch[i].seriesId;
                var dataIndices = modelUtil.normalizeToArray(sourceBatch[i].dataIndex);
                var otherDataIndices = otherMap && otherMap[seriesId];

                for (var j = 0, lenj = dataIndices.length; j < lenj; j++) {
                    var dataIndex = dataIndices[j];

                    if (otherDataIndices && otherDataIndices[dataIndex]) {
                        otherDataIndices[dataIndex] = null;
                    }
                    else {
                        (map[seriesId] || (map[seriesId] = {}))[dataIndex] = 1;
                    }
                }
            }
        }

        function mapToArray(map, isData) {
            var result = [];
            for (var i in map) {
                if (map.hasOwnProperty(i) && map[i] != null) {
                    if (isData) {
                        result.push(+i);
                    }
                    else {
                        var dataIndices = mapToArray(map[i], true);
                        dataIndices.length && result.push({seriesId: i, dataIndex: dataIndices});
                    }
                }
            }
            return result;
        }
    };

    /**
     * @param {module:echarts/data/List} data
     * @param {Object} payload Contains dataIndex (means rawIndex) / dataIndexInside / name
     *                         each of which can be Array or primary type.
     * @return {number|Array.<number>} dataIndex If not found, return undefined/null.
     */
    modelUtil.queryDataIndex = function (data, payload) {
        if (payload.dataIndexInside != null) {
            return payload.dataIndexInside;
        }
        else if (payload.dataIndex != null) {
            return zrUtil.isArray(payload.dataIndex)
                ? zrUtil.map(payload.dataIndex, function (value) {
                    return data.indexOfRawIndex(value);
                })
                : data.indexOfRawIndex(payload.dataIndex);
        }
        else if (payload.name != null) {
            return zrUtil.isArray(payload.name)
                ? zrUtil.map(payload.name, function (value) {
                    return data.indexOfName(value);
                })
                : data.indexOfName(payload.name);
        }
    };

    /**
     * Enable property storage to any host object.
     * Notice: Serialization is not supported.
     *
     * For example:
     * var get = modelUitl.makeGetter();
     *
     * function some(hostObj) {
     *      get(hostObj)._someProperty = 1212;
     *      ...
     * }
     *
     * @return {Function}
     */
    modelUtil.makeGetter = (function () {
        var index = 0;
        return function () {
            var key = '\0__ec_prop_getter_' + index++;
            return function (hostObj) {
                return hostObj[key] || (hostObj[key] = {});
            };
        };
    })();

    /**
     * @param {module:echarts/model/Global} ecModel
     * @param {string|Object} finder
     *        If string, e.g., 'geo', means {geoIndex: 0}.
     *        If Object, could contain some of these properties below:
     *        {
     *            seriesIndex, seriesId, seriesName,
     *            geoIndex, geoId, geoName,
     *            bmapIndex, bmapId, bmapName,
     *            xAxisIndex, xAxisId, xAxisName,
     *            yAxisIndex, yAxisId, yAxisName,
     *            gridIndex, gridId, gridName,
     *            ... (can be extended)
     *        }
     *        Each properties can be number|string|Array.<number>|Array.<string>
     *        For example, a finder could be
     *        {
     *            seriesIndex: 3,
     *            geoId: ['aa', 'cc'],
     *            gridName: ['xx', 'rr']
     *        }
     *        xxxIndex can be set as 'all' (means all xxx) or 'none' (means not specify)
     *        If nothing or null/undefined specified, return nothing.
     * @param {Object} [opt]
     * @param {string} [opt.defaultMainType]
     * @param {Array.<string>} [opt.includeMainTypes]
     * @return {Object} result like:
     *        {
     *            seriesModels: [seriesModel1, seriesModel2],
     *            seriesModel: seriesModel1, // The first model
     *            geoModels: [geoModel1, geoModel2],
     *            geoModel: geoModel1, // The first model
     *            ...
     *        }
     */
    modelUtil.parseFinder = function (ecModel, finder, opt) {
        if (zrUtil.isString(finder)) {
            var obj = {};
            obj[finder + 'Index'] = 0;
            finder = obj;
        }

        var defaultMainType = opt && opt.defaultMainType;
        if (defaultMainType
            && !has(finder, defaultMainType + 'Index')
            && !has(finder, defaultMainType + 'Id')
            && !has(finder, defaultMainType + 'Name')
        ) {
            finder[defaultMainType + 'Index'] = 0;
        }

        var result = {};

        each(finder, function (value, key) {
            var value = finder[key];

            // Exclude 'dataIndex' and other illgal keys.
            if (key === 'dataIndex' || key === 'dataIndexInside') {
                result[key] = value;
                return;
            }

            var parsedKey = key.match(/^(\w+)(Index|Id|Name)$/) || [];
            var mainType = parsedKey[1];
            var queryType = (parsedKey[2] || '').toLowerCase();

            if (!mainType
                || !queryType
                || value == null
                || (queryType === 'index' && value === 'none')
                || (opt && opt.includeMainTypes && zrUtil.indexOf(opt.includeMainTypes, mainType) < 0)
            ) {
                return;
            }

            var queryParam = {mainType: mainType};
            if (queryType !== 'index' || value !== 'all') {
                queryParam[queryType] = value;
            }

            var models = ecModel.queryComponents(queryParam);
            result[mainType + 'Models'] = models;
            result[mainType + 'Model'] = models[0];
        });

        return result;
    };

    /**
     * @see {module:echarts/data/helper/completeDimensions}
     * @param {module:echarts/data/List} data
     * @param {string|number} dataDim
     * @return {string}
     */
    modelUtil.dataDimToCoordDim = function (data, dataDim) {
        var dimensions = data.dimensions;
        dataDim = data.getDimension(dataDim);
        for (var i = 0; i < dimensions.length; i++) {
            var dimItem = data.getDimensionInfo(dimensions[i]);
            if (dimItem.name === dataDim) {
                return dimItem.coordDim;
            }
        }
    };

    /**
     * @see {module:echarts/data/helper/completeDimensions}
     * @param {module:echarts/data/List} data
     * @param {string} coordDim
     * @return {Array.<string>} data dimensions on the coordDim.
     */
    modelUtil.coordDimToDataDim = function (data, coordDim) {
        var dataDim = [];
        each(data.dimensions, function (dimName) {
            var dimItem = data.getDimensionInfo(dimName);
            if (dimItem.coordDim === coordDim) {
                dataDim[dimItem.coordDimIndex] = dimItem.name;
            }
        });
        return dataDim;
    };

    /**
     * @see {module:echarts/data/helper/completeDimensions}
     * @param {module:echarts/data/List} data
     * @param {string} otherDim Can be `otherDims`
     *                        like 'label' or 'tooltip'.
     * @return {Array.<string>} data dimensions on the otherDim.
     */
    modelUtil.otherDimToDataDim = function (data, otherDim) {
        var dataDim = [];
        each(data.dimensions, function (dimName) {
            var dimItem = data.getDimensionInfo(dimName);
            var otherDims = dimItem.otherDims;
            var dimIndex = otherDims[otherDim];
            if (dimIndex != null && dimIndex !== false) {
                dataDim[dimIndex] = dimItem.name;
            }
        });
        return dataDim;
    };

    function has(obj, prop) {
        return obj && obj.hasOwnProperty(prop);
    }

    module.exports = modelUtil;



/***/ }),
/* 28 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * @module echarts/model/Model
 */


    var zrUtil = __webpack_require__(0);
    var clazzUtil = __webpack_require__(29);
    var env = __webpack_require__(13);

    /**
     * @alias module:echarts/model/Model
     * @constructor
     * @param {Object} option
     * @param {module:echarts/model/Model} [parentModel]
     * @param {module:echarts/model/Global} [ecModel]
     */
    function Model(option, parentModel, ecModel) {
        /**
         * @type {module:echarts/model/Model}
         * @readOnly
         */
        this.parentModel = parentModel;

        /**
         * @type {module:echarts/model/Global}
         * @readOnly
         */
        this.ecModel = ecModel;

        /**
         * @type {Object}
         * @protected
         */
        this.option = option;

        // Simple optimization
        // if (this.init) {
        //     if (arguments.length <= 4) {
        //         this.init(option, parentModel, ecModel, extraOpt);
        //     }
        //     else {
        //         this.init.apply(this, arguments);
        //     }
        // }
    }

    Model.prototype = {

        constructor: Model,

        /**
         * Model 的初始化函数
         * @param {Object} option
         */
        init: null,

        /**
         * 从新的 Option merge
         */
        mergeOption: function (option) {
            zrUtil.merge(this.option, option, true);
        },

        /**
         * @param {string|Array.<string>} path
         * @param {boolean} [ignoreParent=false]
         * @return {*}
         */
        get: function (path, ignoreParent) {
            if (path == null) {
                return this.option;
            }

            return doGet(
                this.option,
                this.parsePath(path),
                !ignoreParent && getParent(this, path)
            );
        },

        /**
         * @param {string} key
         * @param {boolean} [ignoreParent=false]
         * @return {*}
         */
        getShallow: function (key, ignoreParent) {
            var option = this.option;

            var val = option == null ? option : option[key];
            var parentModel = !ignoreParent && getParent(this, key);
            if (val == null && parentModel) {
                val = parentModel.getShallow(key);
            }
            return val;
        },

        /**
         * @param {string|Array.<string>} [path]
         * @param {module:echarts/model/Model} [parentModel]
         * @return {module:echarts/model/Model}
         */
        getModel: function (path, parentModel) {
            var obj = path == null
                ? this.option
                : doGet(this.option, path = this.parsePath(path));

            var thisParentModel;
            parentModel = parentModel || (
                (thisParentModel = getParent(this, path))
                    && thisParentModel.getModel(path)
            );

            return new Model(obj, parentModel, this.ecModel);
        },

        /**
         * If model has option
         */
        isEmpty: function () {
            return this.option == null;
        },

        restoreData: function () {},

        // Pending
        clone: function () {
            var Ctor = this.constructor;
            return new Ctor(zrUtil.clone(this.option));
        },

        setReadOnly: function (properties) {
            clazzUtil.setReadOnly(this, properties);
        },

        // If path is null/undefined, return null/undefined.
        parsePath: function(path) {
            if (typeof path === 'string') {
                path = path.split('.');
            }
            return path;
        },

        /**
         * @param {Function} getParentMethod
         *        param {Array.<string>|string} path
         *        return {module:echarts/model/Model}
         */
        customizeGetParent: function (getParentMethod) {
            clazzUtil.set(this, 'getParent', getParentMethod);
        },

        isAnimationEnabled: function () {
            if (!env.node) {
                if (this.option.animation != null) {
                    return !!this.option.animation;
                }
                else if (this.parentModel) {
                    return this.parentModel.isAnimationEnabled();
                }
            }
        }
    };

    function doGet(obj, pathArr, parentModel) {
        for (var i = 0; i < pathArr.length; i++) {
            // Ignore empty
            if (!pathArr[i]) {
                continue;
            }
            // obj could be number/string/... (like 0)
            obj = (obj && typeof obj === 'object') ? obj[pathArr[i]] : null;
            if (obj == null) {
                break;
            }
        }
        if (obj == null && parentModel) {
            obj = parentModel.get(pathArr);
        }
        return obj;
    }

    // `path` can be null/undefined
    function getParent(model, path) {
        var getParentMethod = clazzUtil.get(model, 'getParent');
        return getParentMethod ? getParentMethod.call(model, path) : model.parentModel;
    }

    // Enable Model.extend.
    clazzUtil.enableClassExtend(Model);

    var mixin = zrUtil.mixin;
    mixin(Model, __webpack_require__(30));
    mixin(Model, __webpack_require__(31));
    mixin(Model, __webpack_require__(32));
    mixin(Model, __webpack_require__(71));

    module.exports = Model;


/***/ }),
/* 29 */
/***/ (function(module, exports, __webpack_require__) {



    var zrUtil = __webpack_require__(0);

    var clazz = {};

    var TYPE_DELIMITER = '.';
    var IS_CONTAINER = '___EC__COMPONENT__CONTAINER___';
    var MEMBER_PRIFIX = '\0ec_\0';

    /**
     * Hide private class member.
     * The same behavior as `host[name] = value;` (can be right-value)
     * @public
     */
    clazz.set = function (host, name, value) {
        return (host[MEMBER_PRIFIX + name] = value);
    };

    /**
     * Hide private class member.
     * The same behavior as `host[name];`
     * @public
     */
    clazz.get = function (host, name) {
        return host[MEMBER_PRIFIX + name];
    };

    /**
     * For hidden private class member.
     * The same behavior as `host.hasOwnProperty(name);`
     * @public
     */
    clazz.hasOwn = function (host, name) {
        return host.hasOwnProperty(MEMBER_PRIFIX + name);
    };

    /**
     * Notice, parseClassType('') should returns {main: '', sub: ''}
     * @public
     */
    var parseClassType = clazz.parseClassType = function (componentType) {
        var ret = {main: '', sub: ''};
        if (componentType) {
            componentType = componentType.split(TYPE_DELIMITER);
            ret.main = componentType[0] || '';
            ret.sub = componentType[1] || '';
        }
        return ret;
    };

    /**
     * @public
     */
    function checkClassType(componentType) {
        zrUtil.assert(
            /^[a-zA-Z0-9_]+([.][a-zA-Z0-9_]+)?$/.test(componentType),
            'componentType "' + componentType + '" illegal'
        );
    }

    /**
     * @public
     */
    clazz.enableClassExtend = function (RootClass, mandatoryMethods) {

        RootClass.$constructor = RootClass;
        RootClass.extend = function (proto) {

            if (__DEV__) {
                zrUtil.each(mandatoryMethods, function (method) {
                    if (!proto[method]) {
                        console.warn(
                            'Method `' + method + '` should be implemented'
                            + (proto.type ? ' in ' + proto.type : '') + '.'
                        );
                    }
                });
            }

            var superClass = this;
            var ExtendedClass = function () {
                if (!proto.$constructor) {
                    superClass.apply(this, arguments);
                }
                else {
                    proto.$constructor.apply(this, arguments);
                }
            };

            zrUtil.extend(ExtendedClass.prototype, proto);

            ExtendedClass.extend = this.extend;
            ExtendedClass.superCall = superCall;
            ExtendedClass.superApply = superApply;
            zrUtil.inherits(ExtendedClass, this);
            ExtendedClass.superClass = superClass;

            return ExtendedClass;
        };
    };

    // superCall should have class info, which can not be fetch from 'this'.
    // Consider this case:
    // class A has method f,
    // class B inherits class A, overrides method f, f call superApply('f'),
    // class C inherits class B, do not overrides method f,
    // then when method of class C is called, dead loop occured.
    function superCall(context, methodName) {
        var args = zrUtil.slice(arguments, 2);
        return this.superClass.prototype[methodName].apply(context, args);
    }

    function superApply(context, methodName, args) {
        return this.superClass.prototype[methodName].apply(context, args);
    }

    /**
     * @param {Object} entity
     * @param {Object} options
     * @param {boolean} [options.registerWhenExtend]
     * @public
     */
    clazz.enableClassManagement = function (entity, options) {
        options = options || {};

        /**
         * Component model classes
         * key: componentType,
         * value:
         *     componentClass, when componentType is 'xxx'
         *     or Object.<subKey, componentClass>, when componentType is 'xxx.yy'
         * @type {Object}
         */
        var storage = {};

        entity.registerClass = function (Clazz, componentType) {
            if (componentType) {
                checkClassType(componentType);
                componentType = parseClassType(componentType);

                if (!componentType.sub) {
                    if (__DEV__) {
                        if (storage[componentType.main]) {
                            console.warn(componentType.main + ' exists.');
                        }
                    }
                    storage[componentType.main] = Clazz;
                }
                else if (componentType.sub !== IS_CONTAINER) {
                    var container = makeContainer(componentType);
                    container[componentType.sub] = Clazz;
                }
            }
            return Clazz;
        };

        entity.getClass = function (componentMainType, subType, throwWhenNotFound) {
            var Clazz = storage[componentMainType];

            if (Clazz && Clazz[IS_CONTAINER]) {
                Clazz = subType ? Clazz[subType] : null;
            }

            if (throwWhenNotFound && !Clazz) {
                throw new Error(
                    !subType
                        ? componentMainType + '.' + 'type should be specified.'
                        : 'Component ' + componentMainType + '.' + (subType || '') + ' not exists. Load it first.'
                );
            }

            return Clazz;
        };

        entity.getClassesByMainType = function (componentType) {
            componentType = parseClassType(componentType);

            var result = [];
            var obj = storage[componentType.main];

            if (obj && obj[IS_CONTAINER]) {
                zrUtil.each(obj, function (o, type) {
                    type !== IS_CONTAINER && result.push(o);
                });
            }
            else {
                result.push(obj);
            }

            return result;
        };

        entity.hasClass = function (componentType) {
            // Just consider componentType.main.
            componentType = parseClassType(componentType);
            return !!storage[componentType.main];
        };

        /**
         * @return {Array.<string>} Like ['aa', 'bb'], but can not be ['aa.xx']
         */
        entity.getAllClassMainTypes = function () {
            var types = [];
            zrUtil.each(storage, function (obj, type) {
                types.push(type);
            });
            return types;
        };

        /**
         * If a main type is container and has sub types
         * @param  {string}  mainType
         * @return {boolean}
         */
        entity.hasSubTypes = function (componentType) {
            componentType = parseClassType(componentType);
            var obj = storage[componentType.main];
            return obj && obj[IS_CONTAINER];
        };

        entity.parseClassType = parseClassType;

        function makeContainer(componentType) {
            var container = storage[componentType.main];
            if (!container || !container[IS_CONTAINER]) {
                container = storage[componentType.main] = {};
                container[IS_CONTAINER] = true;
            }
            return container;
        }

        if (options.registerWhenExtend) {
            var originalExtend = entity.extend;
            if (originalExtend) {
                entity.extend = function (proto) {
                    var ExtendedClass = originalExtend.call(this, proto);
                    return entity.registerClass(ExtendedClass, proto.type);
                };
            }
        }

        return entity;
    };

    /**
     * @param {string|Array.<string>} properties
     */
    clazz.setReadOnly = function (obj, properties) {
        // FIXME It seems broken in IE8 simulation of IE11
        // if (!zrUtil.isArray(properties)) {
        //     properties = properties != null ? [properties] : [];
        // }
        // zrUtil.each(properties, function (prop) {
        //     var value = obj[prop];

        //     Object.defineProperty
        //         && Object.defineProperty(obj, prop, {
        //             value: value, writable: false
        //         });
        //     zrUtil.isArray(obj[prop])
        //         && Object.freeze
        //         && Object.freeze(obj[prop]);
        // });
    };

    module.exports = clazz;


/***/ }),
/* 30 */
/***/ (function(module, exports, __webpack_require__) {


    var getLineStyle = __webpack_require__(10)(
        [
            ['lineWidth', 'width'],
            ['stroke', 'color'],
            ['opacity'],
            ['shadowBlur'],
            ['shadowOffsetX'],
            ['shadowOffsetY'],
            ['shadowColor']
        ]
    );
    module.exports = {
        getLineStyle: function (excludes) {
            var style = getLineStyle.call(this, excludes);
            var lineDash = this.getLineDash(style.lineWidth);
            lineDash && (style.lineDash = lineDash);
            return style;
        },

        getLineDash: function (lineWidth) {
            if (lineWidth == null) {
                lineWidth = 1;
            }
            var lineType = this.get('type');
            var dotSize = Math.max(lineWidth, 2);
            var dashSize = lineWidth * 4;
            return (lineType === 'solid' || lineType == null) ? null
                : (lineType === 'dashed' ? [dashSize, dashSize] : [dotSize, dotSize]);
        }
    };


/***/ }),
/* 31 */
/***/ (function(module, exports, __webpack_require__) {


    module.exports = {
        getAreaStyle: __webpack_require__(10)(
            [
                ['fill', 'color'],
                ['shadowBlur'],
                ['shadowOffsetX'],
                ['shadowOffsetY'],
                ['opacity'],
                ['shadowColor']
            ]
        )
    };


/***/ }),
/* 32 */
/***/ (function(module, exports, __webpack_require__) {



    var textContain = __webpack_require__(5);
    var graphicUtil = __webpack_require__(33);

    module.exports = {
        /**
         * Get color property or get color from option.textStyle.color
         * @return {string}
         */
        getTextColor: function () {
            var ecModel = this.ecModel;
            return this.getShallow('color')
                || (ecModel && ecModel.get('textStyle.color'));
        },

        /**
         * Create font string from fontStyle, fontWeight, fontSize, fontFamily
         * @return {string}
         */
        getFont: function () {
            return graphicUtil.getFont({
                fontStyle: this.getShallow('fontStyle'),
                fontWeight: this.getShallow('fontWeight'),
                fontSize: this.getShallow('fontSize'),
                fontFamily: this.getShallow('fontFamily')
            }, this.ecModel);
        },

        getTextRect: function (text) {
            return textContain.getBoundingRect(
                text,
                this.getFont(),
                this.getShallow('align'),
                this.getShallow('baseline')
            );
        },

        truncateText: function (text, containerWidth, ellipsis, options) {
            return textContain.truncateText(
                text, containerWidth, this.getFont(), ellipsis, options
            );
        }
    };


/***/ }),
/* 33 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";



    var zrUtil = __webpack_require__(0);

    var pathTool = __webpack_require__(34);
    var Path = __webpack_require__(1);
    var colorTool = __webpack_require__(16);
    var matrix = __webpack_require__(8);
    var vector = __webpack_require__(2);
    var Transformable = __webpack_require__(15);
    var BoundingRect = __webpack_require__(3);

    var round = Math.round;
    var mathMax = Math.max;
    var mathMin = Math.min;

    var graphic = {};

    graphic.Group = __webpack_require__(53);

    graphic.Image = __webpack_require__(54);

    graphic.Text = __webpack_require__(55);

    graphic.Circle = __webpack_require__(56);

    graphic.Sector = __webpack_require__(57);

    graphic.Ring = __webpack_require__(58);

    graphic.Polygon = __webpack_require__(59);

    graphic.Polyline = __webpack_require__(62);

    graphic.Rect = __webpack_require__(63);

    graphic.Line = __webpack_require__(65);

    graphic.BezierCurve = __webpack_require__(66);

    graphic.Arc = __webpack_require__(67);

    graphic.CompoundPath = __webpack_require__(68);

    graphic.LinearGradient = __webpack_require__(69);

    graphic.RadialGradient = __webpack_require__(70);

    graphic.BoundingRect = BoundingRect;

    /**
     * Extend shape with parameters
     */
    graphic.extendShape = function (opts) {
        return Path.extend(opts);
    };

    /**
     * Extend path
     */
    graphic.extendPath = function (pathData, opts) {
        return pathTool.extendFromString(pathData, opts);
    };

    /**
     * Create a path element from path data string
     * @param {string} pathData
     * @param {Object} opts
     * @param {module:zrender/core/BoundingRect} rect
     * @param {string} [layout=cover] 'center' or 'cover'
     */
    graphic.makePath = function (pathData, opts, rect, layout) {
        var path = pathTool.createFromString(pathData, opts);
        var boundingRect = path.getBoundingRect();
        if (rect) {
            var aspect = boundingRect.width / boundingRect.height;

            if (layout === 'center') {
                // Set rect to center, keep width / height ratio.
                var width = rect.height * aspect;
                var height;
                if (width <= rect.width) {
                    height = rect.height;
                }
                else {
                    width = rect.width;
                    height = width / aspect;
                }
                var cx = rect.x + rect.width / 2;
                var cy = rect.y + rect.height / 2;

                rect.x = cx - width / 2;
                rect.y = cy - height / 2;
                rect.width = width;
                rect.height = height;
            }

            graphic.resizePath(path, rect);
        }
        return path;
    };

    graphic.mergePath = pathTool.mergePath,

    /**
     * Resize a path to fit the rect
     * @param {module:zrender/graphic/Path} path
     * @param {Object} rect
     */
    graphic.resizePath = function (path, rect) {
        if (!path.applyTransform) {
            return;
        }

        var pathRect = path.getBoundingRect();

        var m = pathRect.calculateTransform(rect);

        path.applyTransform(m);
    };

    /**
     * Sub pixel optimize line for canvas
     *
     * @param {Object} param
     * @param {Object} [param.shape]
     * @param {number} [param.shape.x1]
     * @param {number} [param.shape.y1]
     * @param {number} [param.shape.x2]
     * @param {number} [param.shape.y2]
     * @param {Object} [param.style]
     * @param {number} [param.style.lineWidth]
     * @return {Object} Modified param
     */
    graphic.subPixelOptimizeLine = function (param) {
        var subPixelOptimize = graphic.subPixelOptimize;
        var shape = param.shape;
        var lineWidth = param.style.lineWidth;

        if (round(shape.x1 * 2) === round(shape.x2 * 2)) {
            shape.x1 = shape.x2 = subPixelOptimize(shape.x1, lineWidth, true);
        }
        if (round(shape.y1 * 2) === round(shape.y2 * 2)) {
            shape.y1 = shape.y2 = subPixelOptimize(shape.y1, lineWidth, true);
        }
        return param;
    };

    /**
     * Sub pixel optimize rect for canvas
     *
     * @param {Object} param
     * @param {Object} [param.shape]
     * @param {number} [param.shape.x]
     * @param {number} [param.shape.y]
     * @param {number} [param.shape.width]
     * @param {number} [param.shape.height]
     * @param {Object} [param.style]
     * @param {number} [param.style.lineWidth]
     * @return {Object} Modified param
     */
    graphic.subPixelOptimizeRect = function (param) {
        var subPixelOptimize = graphic.subPixelOptimize;
        var shape = param.shape;
        var lineWidth = param.style.lineWidth;
        var originX = shape.x;
        var originY = shape.y;
        var originWidth = shape.width;
        var originHeight = shape.height;
        shape.x = subPixelOptimize(shape.x, lineWidth, true);
        shape.y = subPixelOptimize(shape.y, lineWidth, true);
        shape.width = Math.max(
            subPixelOptimize(originX + originWidth, lineWidth, false) - shape.x,
            originWidth === 0 ? 0 : 1
        );
        shape.height = Math.max(
            subPixelOptimize(originY + originHeight, lineWidth, false) - shape.y,
            originHeight === 0 ? 0 : 1
        );
        return param;
    };

    /**
     * Sub pixel optimize for canvas
     *
     * @param {number} position Coordinate, such as x, y
     * @param {number} lineWidth Should be nonnegative integer.
     * @param {boolean=} positiveOrNegative Default false (negative).
     * @return {number} Optimized position.
     */
    graphic.subPixelOptimize = function (position, lineWidth, positiveOrNegative) {
        // Assure that (position + lineWidth / 2) is near integer edge,
        // otherwise line will be fuzzy in canvas.
        var doubledPosition = round(position * 2);
        return (doubledPosition + round(lineWidth)) % 2 === 0
            ? doubledPosition / 2
            : (doubledPosition + (positiveOrNegative ? 1 : -1)) / 2;
    };

    function hasFillOrStroke(fillOrStroke) {
        return fillOrStroke != null && fillOrStroke != 'none';
    }

    function liftColor(color) {
        return typeof color === 'string' ? colorTool.lift(color, -0.1) : color;
    }

    /**
     * @private
     */
    function cacheElementStl(el) {
        if (el.__hoverStlDirty) {
            var stroke = el.style.stroke;
            var fill = el.style.fill;

            // Create hoverStyle on mouseover
            var hoverStyle = el.__hoverStl;
            hoverStyle.fill = hoverStyle.fill
                || (hasFillOrStroke(fill) ? liftColor(fill) : null);
            hoverStyle.stroke = hoverStyle.stroke
                || (hasFillOrStroke(stroke) ? liftColor(stroke) : null);

            var normalStyle = {};
            for (var name in hoverStyle) {
                if (hoverStyle.hasOwnProperty(name)) {
                    normalStyle[name] = el.style[name];
                }
            }

            el.__normalStl = normalStyle;

            el.__hoverStlDirty = false;
        }
    }

    /**
     * @private
     */
    function doSingleEnterHover(el) {
        if (el.__isHover) {
            return;
        }

        cacheElementStl(el);

        if (el.useHoverLayer) {
            el.__zr && el.__zr.addHover(el, el.__hoverStl);
        }
        else {
            el.setStyle(el.__hoverStl);
            el.z2 += 1;
        }

        el.__isHover = true;
    }

    /**
     * @inner
     */
    function doSingleLeaveHover(el) {
        if (!el.__isHover) {
            return;
        }

        var normalStl = el.__normalStl;
        if (el.useHoverLayer) {
            el.__zr && el.__zr.removeHover(el);
        }
        else {
            normalStl && el.setStyle(normalStl);
            el.z2 -= 1;
        }

        el.__isHover = false;
    }

    /**
     * @inner
     */
    function doEnterHover(el) {
        el.type === 'group'
            ? el.traverse(function (child) {
                if (child.type !== 'group') {
                    doSingleEnterHover(child);
                }
            })
            : doSingleEnterHover(el);
    }

    function doLeaveHover(el) {
        el.type === 'group'
            ? el.traverse(function (child) {
                if (child.type !== 'group') {
                    doSingleLeaveHover(child);
                }
            })
            : doSingleLeaveHover(el);
    }

    /**
     * @inner
     */
    function setElementHoverStl(el, hoverStl) {
        // If element has sepcified hoverStyle, then use it instead of given hoverStyle
        // Often used when item group has a label element and it's hoverStyle is different
        el.__hoverStl = el.hoverStyle || hoverStl || {};
        el.__hoverStlDirty = true;

        if (el.__isHover) {
            cacheElementStl(el);
        }
    }

    /**
     * @inner
     */
    function onElementMouseOver(e) {
        if (this.__hoverSilentOnTouch && e.zrByTouch) {
            return;
        }

        // Only if element is not in emphasis status
        !this.__isEmphasis && doEnterHover(this);
    }

    /**
     * @inner
     */
    function onElementMouseOut(e) {
        if (this.__hoverSilentOnTouch && e.zrByTouch) {
            return;
        }

        // Only if element is not in emphasis status
        !this.__isEmphasis && doLeaveHover(this);
    }

    /**
     * @inner
     */
    function enterEmphasis() {
        this.__isEmphasis = true;
        doEnterHover(this);
    }

    /**
     * @inner
     */
    function leaveEmphasis() {
        this.__isEmphasis = false;
        doLeaveHover(this);
    }

    /**
     * Set hover style of element.
     * This method can be called repeatly without side-effects.
     * @param {module:zrender/Element} el
     * @param {Object} [hoverStyle]
     * @param {Object} [opt]
     * @param {boolean} [opt.hoverSilentOnTouch=false]
     *        In touch device, mouseover event will be trigger on touchstart event
     *        (see module:zrender/dom/HandlerProxy). By this mechanism, we can
     *        conviniently use hoverStyle when tap on touch screen without additional
     *        code for compatibility.
     *        But if the chart/component has select feature, which usually also use
     *        hoverStyle, there might be conflict between 'select-highlight' and
     *        'hover-highlight' especially when roam is enabled (see geo for example).
     *        In this case, hoverSilentOnTouch should be used to disable hover-highlight
     *        on touch device.
     */
    graphic.setHoverStyle = function (el, hoverStyle, opt) {
        el.__hoverSilentOnTouch = opt && opt.hoverSilentOnTouch;

        el.type === 'group'
            ? el.traverse(function (child) {
                if (child.type !== 'group') {
                    setElementHoverStl(child, hoverStyle);
                }
            })
            : setElementHoverStl(el, hoverStyle);

        // Duplicated function will be auto-ignored, see Eventful.js.
        el.on('mouseover', onElementMouseOver)
          .on('mouseout', onElementMouseOut);

        // Emphasis, normal can be triggered manually
        el.on('emphasis', enterEmphasis)
          .on('normal', leaveEmphasis);
    };

    /**
     * Set text option in the style
     * @param {Object} textStyle
     * @param {module:echarts/model/Model} labelModel
     * @param {string} color
     */
    graphic.setText = function (textStyle, labelModel, color) {
        var labelPosition = labelModel.getShallow('position') || 'inside';
        var labelOffset = labelModel.getShallow('offset');
        var labelColor = labelPosition.indexOf('inside') >= 0 ? 'white' : color;
        var textStyleModel = labelModel.getModel('textStyle');
        zrUtil.extend(textStyle, {
            textDistance: labelModel.getShallow('distance') || 5,
            textFont: textStyleModel.getFont(),
            textPosition: labelPosition,
            textOffset: labelOffset,
            textFill: textStyleModel.getTextColor() || labelColor
        });
    };

    graphic.getFont = function (opt, ecModel) {
        var gTextStyleModel = ecModel && ecModel.getModel('textStyle');
        return [
            // FIXME in node-canvas fontWeight is before fontStyle
            opt.fontStyle || gTextStyleModel && gTextStyleModel.getShallow('fontStyle') || '',
            opt.fontWeight || gTextStyleModel && gTextStyleModel.getShallow('fontWeight') || '',
            (opt.fontSize || gTextStyleModel && gTextStyleModel.getShallow('fontSize') || 12) + 'px',
            opt.fontFamily || gTextStyleModel && gTextStyleModel.getShallow('fontFamily') || 'sans-serif'
        ].join(' ');
    };

    function animateOrSetProps(isUpdate, el, props, animatableModel, dataIndex, cb) {
        if (typeof dataIndex === 'function') {
            cb = dataIndex;
            dataIndex = null;
        }
        // Do not check 'animation' property directly here. Consider this case:
        // animation model is an `itemModel`, whose does not have `isAnimationEnabled`
        // but its parent model (`seriesModel`) does.
        var animationEnabled = animatableModel && animatableModel.isAnimationEnabled();

        if (animationEnabled) {
            var postfix = isUpdate ? 'Update' : '';
            var duration = animatableModel.getShallow('animationDuration' + postfix);
            var animationEasing = animatableModel.getShallow('animationEasing' + postfix);
            var animationDelay = animatableModel.getShallow('animationDelay' + postfix);
            if (typeof animationDelay === 'function') {
                animationDelay = animationDelay(
                    dataIndex,
                    animatableModel.getAnimationDelayParams
                        ? animatableModel.getAnimationDelayParams(el, dataIndex)
                        : null
                );
            }
            if (typeof duration === 'function') {
                duration = duration(dataIndex);
            }

            duration > 0
                ? el.animateTo(props, duration, animationDelay || 0, animationEasing, cb)
                : (el.stopAnimation(), el.attr(props), cb && cb());
        }
        else {
            el.stopAnimation();
            el.attr(props);
            cb && cb();
        }
    }

    /**
     * Update graphic element properties with or without animation according to the configuration in series
     * @param {module:zrender/Element} el
     * @param {Object} props
     * @param {module:echarts/model/Model} [animatableModel]
     * @param {number} [dataIndex]
     * @param {Function} [cb]
     * @example
     *     graphic.updateProps(el, {
     *         position: [100, 100]
     *     }, seriesModel, dataIndex, function () { console.log('Animation done!'); });
     *     // Or
     *     graphic.updateProps(el, {
     *         position: [100, 100]
     *     }, seriesModel, function () { console.log('Animation done!'); });
     */
    graphic.updateProps = function (el, props, animatableModel, dataIndex, cb) {
        animateOrSetProps(true, el, props, animatableModel, dataIndex, cb);
    };

    /**
     * Init graphic element properties with or without animation according to the configuration in series
     * @param {module:zrender/Element} el
     * @param {Object} props
     * @param {module:echarts/model/Model} [animatableModel]
     * @param {number} [dataIndex]
     * @param {Function} cb
     */
    graphic.initProps = function (el, props, animatableModel, dataIndex, cb) {
        animateOrSetProps(false, el, props, animatableModel, dataIndex, cb);
    };

    /**
     * Get transform matrix of target (param target),
     * in coordinate of its ancestor (param ancestor)
     *
     * @param {module:zrender/mixin/Transformable} target
     * @param {module:zrender/mixin/Transformable} [ancestor]
     */
    graphic.getTransform = function (target, ancestor) {
        var mat = matrix.identity([]);

        while (target && target !== ancestor) {
            matrix.mul(mat, target.getLocalTransform(), mat);
            target = target.parent;
        }

        return mat;
    };

    /**
     * Apply transform to an vertex.
     * @param {Array.<number>} target [x, y]
     * @param {Array.<number>|TypedArray.<number>|Object} transform Can be:
     *      + Transform matrix: like [1, 0, 0, 1, 0, 0]
     *      + {position, rotation, scale}, the same as `zrender/Transformable`.
     * @param {boolean=} invert Whether use invert matrix.
     * @return {Array.<number>} [x, y]
     */
    graphic.applyTransform = function (target, transform, invert) {
        if (transform && !zrUtil.isArrayLike(transform)) {
            transform = Transformable.getLocalTransform(transform);
        }

        if (invert) {
            transform = matrix.invert([], transform);
        }
        return vector.applyTransform([], target, transform);
    };

    /**
     * @param {string} direction 'left' 'right' 'top' 'bottom'
     * @param {Array.<number>} transform Transform matrix: like [1, 0, 0, 1, 0, 0]
     * @param {boolean=} invert Whether use invert matrix.
     * @return {string} Transformed direction. 'left' 'right' 'top' 'bottom'
     */
    graphic.transformDirection = function (direction, transform, invert) {

        // Pick a base, ensure that transform result will not be (0, 0).
        var hBase = (transform[4] === 0 || transform[5] === 0 || transform[0] === 0)
            ? 1 : Math.abs(2 * transform[4] / transform[0]);
        var vBase = (transform[4] === 0 || transform[5] === 0 || transform[2] === 0)
            ? 1 : Math.abs(2 * transform[4] / transform[2]);

        var vertex = [
            direction === 'left' ? -hBase : direction === 'right' ? hBase : 0,
            direction === 'top' ? -vBase : direction === 'bottom' ? vBase : 0
        ];

        vertex = graphic.applyTransform(vertex, transform, invert);

        return Math.abs(vertex[0]) > Math.abs(vertex[1])
            ? (vertex[0] > 0 ? 'right' : 'left')
            : (vertex[1] > 0 ? 'bottom' : 'top');
    };

    /**
     * Apply group transition animation from g1 to g2.
     * If no animatableModel, no animation.
     */
    graphic.groupTransition = function (g1, g2, animatableModel, cb) {
        if (!g1 || !g2) {
            return;
        }

        function getElMap(g) {
            var elMap = {};
            g.traverse(function (el) {
                if (!el.isGroup && el.anid) {
                    elMap[el.anid] = el;
                }
            });
            return elMap;
        }
        function getAnimatableProps(el) {
            var obj = {
                position: vector.clone(el.position),
                rotation: el.rotation
            };
            if (el.shape) {
                obj.shape = zrUtil.extend({}, el.shape);
            }
            return obj;
        }
        var elMap1 = getElMap(g1);

        g2.traverse(function (el) {
            if (!el.isGroup && el.anid) {
                var oldEl = elMap1[el.anid];
                if (oldEl) {
                    var newProp = getAnimatableProps(el);
                    el.attr(getAnimatableProps(oldEl));
                    graphic.updateProps(el, newProp, animatableModel, el.dataIndex);
                }
                // else {
                //     if (el.previousProps) {
                //         graphic.updateProps
                //     }
                // }
            }
        });
    };

    /**
     * @param {Array.<Array.<number>>} points Like: [[23, 44], [53, 66], ...]
     * @param {Object} rect {x, y, width, height}
     * @return {Array.<Array.<number>>} A new clipped points.
     */
    graphic.clipPointsByRect = function (points, rect) {
        return zrUtil.map(points, function (point) {
            var x = point[0];
            x = mathMax(x, rect.x);
            x = mathMin(x, rect.x + rect.width);
            var y = point[1];
            y = mathMax(y, rect.y);
            y = mathMin(y, rect.y + rect.height);
            return [x, y];
        });
    };

    /**
     * @param {Object} targetRect {x, y, width, height}
     * @param {Object} rect {x, y, width, height}
     * @return {Object} A new clipped rect. If rect size are negative, return undefined.
     */
    graphic.clipRectByRect = function (targetRect, rect) {
        var x = mathMax(targetRect.x, rect.x);
        var x2 = mathMin(targetRect.x + targetRect.width, rect.x + rect.width);
        var y = mathMax(targetRect.y, rect.y);
        var y2 = mathMin(targetRect.y + targetRect.height, rect.y + rect.height);

        if (x2 >= x && y2 >= y) {
            return {
                x: x,
                y: y,
                width: x2 - x,
                height: y2 - y
            };
        }
    };

    module.exports = graphic;



/***/ }),
/* 34 */
/***/ (function(module, exports, __webpack_require__) {



    var Path = __webpack_require__(1);
    var PathProxy = __webpack_require__(6);
    var transformPath = __webpack_require__(52);

    // command chars
    var cc = [
        'm', 'M', 'l', 'L', 'v', 'V', 'h', 'H', 'z', 'Z',
        'c', 'C', 'q', 'Q', 't', 'T', 's', 'S', 'a', 'A'
    ];

    var mathSqrt = Math.sqrt;
    var mathSin = Math.sin;
    var mathCos = Math.cos;
    var PI = Math.PI;

    var vMag = function(v) {
        return Math.sqrt(v[0] * v[0] + v[1] * v[1]);
    };
    var vRatio = function(u, v) {
        return (u[0] * v[0] + u[1] * v[1]) / (vMag(u) * vMag(v));
    };
    var vAngle = function(u, v) {
        return (u[0] * v[1] < u[1] * v[0] ? -1 : 1)
                * Math.acos(vRatio(u, v));
    };

    function processArc(x1, y1, x2, y2, fa, fs, rx, ry, psiDeg, cmd, path) {
        var psi = psiDeg * (PI / 180.0);
        var xp = mathCos(psi) * (x1 - x2) / 2.0
                 + mathSin(psi) * (y1 - y2) / 2.0;
        var yp = -1 * mathSin(psi) * (x1 - x2) / 2.0
                 + mathCos(psi) * (y1 - y2) / 2.0;

        var lambda = (xp * xp) / (rx * rx) + (yp * yp) / (ry * ry);

        if (lambda > 1) {
            rx *= mathSqrt(lambda);
            ry *= mathSqrt(lambda);
        }

        var f = (fa === fs ? -1 : 1)
            * mathSqrt((((rx * rx) * (ry * ry))
                    - ((rx * rx) * (yp * yp))
                    - ((ry * ry) * (xp * xp))) / ((rx * rx) * (yp * yp)
                    + (ry * ry) * (xp * xp))
                ) || 0;

        var cxp = f * rx * yp / ry;
        var cyp = f * -ry * xp / rx;

        var cx = (x1 + x2) / 2.0
                 + mathCos(psi) * cxp
                 - mathSin(psi) * cyp;
        var cy = (y1 + y2) / 2.0
                + mathSin(psi) * cxp
                + mathCos(psi) * cyp;

        var theta = vAngle([ 1, 0 ], [ (xp - cxp) / rx, (yp - cyp) / ry ]);
        var u = [ (xp - cxp) / rx, (yp - cyp) / ry ];
        var v = [ (-1 * xp - cxp) / rx, (-1 * yp - cyp) / ry ];
        var dTheta = vAngle(u, v);

        if (vRatio(u, v) <= -1) {
            dTheta = PI;
        }
        if (vRatio(u, v) >= 1) {
            dTheta = 0;
        }
        if (fs === 0 && dTheta > 0) {
            dTheta = dTheta - 2 * PI;
        }
        if (fs === 1 && dTheta < 0) {
            dTheta = dTheta + 2 * PI;
        }

        path.addData(cmd, cx, cy, rx, ry, theta, dTheta, psi, fs);
    }

    function createPathProxyFromString(data) {
        if (!data) {
            return [];
        }

        // command string
        var cs = data.replace(/-/g, ' -')
            .replace(/  /g, ' ')
            .replace(/ /g, ',')
            .replace(/,,/g, ',');

        var n;
        // create pipes so that we can split the data
        for (n = 0; n < cc.length; n++) {
            cs = cs.replace(new RegExp(cc[n], 'g'), '|' + cc[n]);
        }

        // create array
        var arr = cs.split('|');
        // init context point
        var cpx = 0;
        var cpy = 0;

        var path = new PathProxy();
        var CMD = PathProxy.CMD;

        var prevCmd;
        for (n = 1; n < arr.length; n++) {
            var str = arr[n];
            var c = str.charAt(0);
            var off = 0;
            var p = str.slice(1).replace(/e,-/g, 'e-').split(',');
            var cmd;

            if (p.length > 0 && p[0] === '') {
                p.shift();
            }

            for (var i = 0; i < p.length; i++) {
                p[i] = parseFloat(p[i]);
            }
            while (off < p.length && !isNaN(p[off])) {
                if (isNaN(p[0])) {
                    break;
                }
                var ctlPtx;
                var ctlPty;

                var rx;
                var ry;
                var psi;
                var fa;
                var fs;

                var x1 = cpx;
                var y1 = cpy;

                // convert l, H, h, V, and v to L
                switch (c) {
                    case 'l':
                        cpx += p[off++];
                        cpy += p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'L':
                        cpx = p[off++];
                        cpy = p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'm':
                        cpx += p[off++];
                        cpy += p[off++];
                        cmd = CMD.M;
                        path.addData(cmd, cpx, cpy);
                        c = 'l';
                        break;
                    case 'M':
                        cpx = p[off++];
                        cpy = p[off++];
                        cmd = CMD.M;
                        path.addData(cmd, cpx, cpy);
                        c = 'L';
                        break;
                    case 'h':
                        cpx += p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'H':
                        cpx = p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'v':
                        cpy += p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'V':
                        cpy = p[off++];
                        cmd = CMD.L;
                        path.addData(cmd, cpx, cpy);
                        break;
                    case 'C':
                        cmd = CMD.C;
                        path.addData(
                            cmd, p[off++], p[off++], p[off++], p[off++], p[off++], p[off++]
                        );
                        cpx = p[off - 2];
                        cpy = p[off - 1];
                        break;
                    case 'c':
                        cmd = CMD.C;
                        path.addData(
                            cmd,
                            p[off++] + cpx, p[off++] + cpy,
                            p[off++] + cpx, p[off++] + cpy,
                            p[off++] + cpx, p[off++] + cpy
                        );
                        cpx += p[off - 2];
                        cpy += p[off - 1];
                        break;
                    case 'S':
                        ctlPtx = cpx;
                        ctlPty = cpy;
                        var len = path.len();
                        var pathData = path.data;
                        if (prevCmd === CMD.C) {
                            ctlPtx += cpx - pathData[len - 4];
                            ctlPty += cpy - pathData[len - 3];
                        }
                        cmd = CMD.C;
                        x1 = p[off++];
                        y1 = p[off++];
                        cpx = p[off++];
                        cpy = p[off++];
                        path.addData(cmd, ctlPtx, ctlPty, x1, y1, cpx, cpy);
                        break;
                    case 's':
                        ctlPtx = cpx;
                        ctlPty = cpy;
                        var len = path.len();
                        var pathData = path.data;
                        if (prevCmd === CMD.C) {
                            ctlPtx += cpx - pathData[len - 4];
                            ctlPty += cpy - pathData[len - 3];
                        }
                        cmd = CMD.C;
                        x1 = cpx + p[off++];
                        y1 = cpy + p[off++];
                        cpx += p[off++];
                        cpy += p[off++];
                        path.addData(cmd, ctlPtx, ctlPty, x1, y1, cpx, cpy);
                        break;
                    case 'Q':
                        x1 = p[off++];
                        y1 = p[off++];
                        cpx = p[off++];
                        cpy = p[off++];
                        cmd = CMD.Q;
                        path.addData(cmd, x1, y1, cpx, cpy);
                        break;
                    case 'q':
                        x1 = p[off++] + cpx;
                        y1 = p[off++] + cpy;
                        cpx += p[off++];
                        cpy += p[off++];
                        cmd = CMD.Q;
                        path.addData(cmd, x1, y1, cpx, cpy);
                        break;
                    case 'T':
                        ctlPtx = cpx;
                        ctlPty = cpy;
                        var len = path.len();
                        var pathData = path.data;
                        if (prevCmd === CMD.Q) {
                            ctlPtx += cpx - pathData[len - 4];
                            ctlPty += cpy - pathData[len - 3];
                        }
                        cpx = p[off++];
                        cpy = p[off++];
                        cmd = CMD.Q;
                        path.addData(cmd, ctlPtx, ctlPty, cpx, cpy);
                        break;
                    case 't':
                        ctlPtx = cpx;
                        ctlPty = cpy;
                        var len = path.len();
                        var pathData = path.data;
                        if (prevCmd === CMD.Q) {
                            ctlPtx += cpx - pathData[len - 4];
                            ctlPty += cpy - pathData[len - 3];
                        }
                        cpx += p[off++];
                        cpy += p[off++];
                        cmd = CMD.Q;
                        path.addData(cmd, ctlPtx, ctlPty, cpx, cpy);
                        break;
                    case 'A':
                        rx = p[off++];
                        ry = p[off++];
                        psi = p[off++];
                        fa = p[off++];
                        fs = p[off++];

                        x1 = cpx, y1 = cpy;
                        cpx = p[off++];
                        cpy = p[off++];
                        cmd = CMD.A;
                        processArc(
                            x1, y1, cpx, cpy, fa, fs, rx, ry, psi, cmd, path
                        );
                        break;
                    case 'a':
                        rx = p[off++];
                        ry = p[off++];
                        psi = p[off++];
                        fa = p[off++];
                        fs = p[off++];

                        x1 = cpx, y1 = cpy;
                        cpx += p[off++];
                        cpy += p[off++];
                        cmd = CMD.A;
                        processArc(
                            x1, y1, cpx, cpy, fa, fs, rx, ry, psi, cmd, path
                        );
                        break;
                }
            }

            if (c === 'z' || c === 'Z') {
                cmd = CMD.Z;
                path.addData(cmd);
            }

            prevCmd = cmd;
        }

        path.toStatic();

        return path;
    }

    // TODO Optimize double memory cost problem
    function createPathOptions(str, opts) {
        var pathProxy = createPathProxyFromString(str);
        opts = opts || {};
        opts.buildPath = function (path) {
            if (path.setData) {
                path.setData(pathProxy.data);
                // Svg and vml renderer don't have context
                var ctx = path.getContext();
                if (ctx) {
                    path.rebuildPath(ctx);
                }
            }
            else {
                var ctx = path;
                pathProxy.rebuildPath(ctx);
            }
        };

        opts.applyTransform = function (m) {
            transformPath(pathProxy, m);

            this.dirty(true);
        };

        return opts;
    }

    module.exports = {
        /**
         * Create a Path object from path string data
         * http://www.w3.org/TR/SVG/paths.html#PathData
         * @param  {Object} opts Other options
         */
        createFromString: function (str, opts) {
            return new Path(createPathOptions(str, opts));
        },

        /**
         * Create a Path class from path string data
         * @param  {string} str
         * @param  {Object} opts Other options
         */
        extendFromString: function (str, opts) {
            return Path.extend(createPathOptions(str, opts));
        },

        /**
         * Merge multiple paths
         */
        // TODO Apply transform
        // TODO stroke dash
        // TODO Optimize double memory cost problem
        mergePath: function (pathEls, opts) {
            var pathList = [];
            var len = pathEls.length;
            for (var i = 0; i < len; i++) {
                var pathEl = pathEls[i];
                if (!pathEl.path) {
                    pathEl.createPathProxy();
                }
                if (pathEl.__dirtyPath) {
                    pathEl.buildPath(pathEl.path, pathEl.shape, true);
                }
                pathList.push(pathEl.path);
            }

            var pathBundle = new Path(opts);
            // Need path proxy.
            pathBundle.createPathProxy();
            pathBundle.buildPath = function (path) {
                path.appendPath(pathList);
                // Svg and vml renderer don't have context
                var ctx = path.getContext();
                if (ctx) {
                    path.rebuildPath(ctx);
                }
            };

            return pathBundle;
        }
    };


/***/ }),
/* 35 */
/***/ (function(module, exports) {

/**
 * @module zrender/graphic/Style
 */


    var STYLE_COMMON_PROPS = [
        ['shadowBlur', 0], ['shadowOffsetX', 0], ['shadowOffsetY', 0], ['shadowColor', '#000'],
        ['lineCap', 'butt'], ['lineJoin', 'miter'], ['miterLimit', 10]
    ];

    // var SHADOW_PROPS = STYLE_COMMON_PROPS.slice(0, 4);
    // var LINE_PROPS = STYLE_COMMON_PROPS.slice(4);

    var Style = function (opts) {
        this.extendFrom(opts);
    };

    function createLinearGradient(ctx, obj, rect) {
        var x = obj.x == null ? 0 : obj.x;
        var x2 = obj.x2 == null ? 1 : obj.x2;
        var y = obj.y == null ? 0 : obj.y;
        var y2 = obj.y2 == null ? 0 : obj.y2;

        if (!obj.global) {
            x = x * rect.width + rect.x;
            x2 = x2 * rect.width + rect.x;
            y = y * rect.height + rect.y;
            y2 = y2 * rect.height + rect.y;
        }

        var canvasGradient = ctx.createLinearGradient(x, y, x2, y2);

        return canvasGradient;
    }

    function createRadialGradient(ctx, obj, rect) {
        var width = rect.width;
        var height = rect.height;
        var min = Math.min(width, height);

        var x = obj.x == null ? 0.5 : obj.x;
        var y = obj.y == null ? 0.5 : obj.y;
        var r = obj.r == null ? 0.5 : obj.r;
        if (!obj.global) {
            x = x * width + rect.x;
            y = y * height + rect.y;
            r = r * min;
        }

        var canvasGradient = ctx.createRadialGradient(x, y, 0, x, y, r);

        return canvasGradient;
    }


    Style.prototype = {

        constructor: Style,

        /**
         * @type {string}
         */
        fill: '#000000',

        /**
         * @type {string}
         */
        stroke: null,

        /**
         * @type {number}
         */
        opacity: 1,

        /**
         * @type {Array.<number>}
         */
        lineDash: null,

        /**
         * @type {number}
         */
        lineDashOffset: 0,

        /**
         * @type {number}
         */
        shadowBlur: 0,

        /**
         * @type {number}
         */
        shadowOffsetX: 0,

        /**
         * @type {number}
         */
        shadowOffsetY: 0,

        /**
         * @type {number}
         */
        lineWidth: 1,

        /**
         * If stroke ignore scale
         * @type {Boolean}
         */
        strokeNoScale: false,

        // Bounding rect text configuration
        // Not affected by element transform
        /**
         * @type {string}
         */
        text: null,

        /**
         * @type {string}
         */
        textFill: '#000',

        /**
         * @type {string}
         */
        textStroke: null,

        /**
         * 'inside', 'left', 'right', 'top', 'bottom'
         * [x, y]
         * @type {string|Array.<number>}
         * @default 'inside'
         */
        textPosition: 'inside',

        /**
         * If not specified, use the boundingRect of a `displayable`.
         * @type {Object}
         */
        textPositionRect: null,

        /**
         * [x, y]
         * @type {Array.<number>}
         */
        textOffset: null,

        /**
         * @type {string}
         */
        textBaseline: null,

        /**
         * @type {string}
         */
        textAlign: null,

        /**
         * @type {string}
         */
        textVerticalAlign: null,

        /**
         * Only useful in Path and Image element
         * @type {number}
         */
        textDistance: 5,

        /**
         * Only useful in Path and Image element
         * @type {number}
         */
        textShadowBlur: 0,

        /**
         * Only useful in Path and Image element
         * @type {number}
         */
        textShadowOffsetX: 0,

        /**
         * Only useful in Path and Image element
         * @type {number}
         */
        textShadowOffsetY: 0,

        /**
         * If transform text
         * Only useful in Path and Image element
         * @type {boolean}
         */
        textTransform: false,

        /**
         * Text rotate around position of Path or Image
         * Only useful in Path and Image element and textTransform is false.
         */
        textRotation: 0,

        /**
         * @type {string}
         * https://developer.mozilla.org/en-US/docs/Web/API/CanvasRenderingContext2D/globalCompositeOperation
         */
        blend: null,

        /**
         * @param {CanvasRenderingContext2D} ctx
         */
        bind: function (ctx, el, prevEl) {
            var style = this;
            var prevStyle = prevEl && prevEl.style;
            var firstDraw = !prevStyle;

            for (var i = 0; i < STYLE_COMMON_PROPS.length; i++) {
                var prop = STYLE_COMMON_PROPS[i];
                var styleName = prop[0];

                if (firstDraw || style[styleName] !== prevStyle[styleName]) {
                    // FIXME Invalid property value will cause style leak from previous element.
                    ctx[styleName] = style[styleName] || prop[1];
                }
            }

            if ((firstDraw || style.fill !== prevStyle.fill)) {
                ctx.fillStyle = style.fill;
            }
            if ((firstDraw || style.stroke !== prevStyle.stroke)) {
                ctx.strokeStyle = style.stroke;
            }
            if ((firstDraw || style.opacity !== prevStyle.opacity)) {
                ctx.globalAlpha = style.opacity == null ? 1 : style.opacity;
            }

            if ((firstDraw || style.blend !== prevStyle.blend)) {
                ctx.globalCompositeOperation = style.blend || 'source-over';
            }
            if (this.hasStroke()) {
                var lineWidth = style.lineWidth;
                ctx.lineWidth = lineWidth / (
                    (this.strokeNoScale && el && el.getLineScale) ? el.getLineScale() : 1
                );
            }
        },

        hasFill: function () {
            var fill = this.fill;
            return fill != null && fill !== 'none';
        },

        hasStroke: function () {
            var stroke = this.stroke;
            return stroke != null && stroke !== 'none' && this.lineWidth > 0;
        },

        /**
         * Extend from other style
         * @param {zrender/graphic/Style} otherStyle
         * @param {boolean} overwrite
         */
        extendFrom: function (otherStyle, overwrite) {
            if (otherStyle) {
                var target = this;
                for (var name in otherStyle) {
                    if (otherStyle.hasOwnProperty(name)
                        && (overwrite || ! target.hasOwnProperty(name))
                    ) {
                        target[name] = otherStyle[name];
                    }
                }
            }
        },

        /**
         * Batch setting style with a given object
         * @param {Object|string} obj
         * @param {*} [obj]
         */
        set: function (obj, value) {
            if (typeof obj === 'string') {
                this[obj] = value;
            }
            else {
                this.extendFrom(obj, true);
            }
        },

        /**
         * Clone
         * @return {zrender/graphic/Style} [description]
         */
        clone: function () {
            var newStyle = new this.constructor();
            newStyle.extendFrom(this, true);
            return newStyle;
        },

        getGradient: function (ctx, obj, rect) {
            var method = obj.type === 'radial' ? createRadialGradient : createLinearGradient;
            var canvasGradient = method(ctx, obj, rect);
            var colorStops = obj.colorStops;
            for (var i = 0; i < colorStops.length; i++) {
                canvasGradient.addColorStop(
                    colorStops[i].offset, colorStops[i].color
                );
            }
            return canvasGradient;
        }
    };

    var styleProto = Style.prototype;
    for (var i = 0; i < STYLE_COMMON_PROPS.length; i++) {
        var prop = STYLE_COMMON_PROPS[i];
        if (!(prop[0] in styleProto)) {
            styleProto[prop[0]] = prop[1];
        }
    }

    // Provide for others
    Style.getGradient = styleProto.getGradient;

    module.exports = Style;


/***/ }),
/* 36 */
/***/ (function(module, exports) {

/**
 * zrender: 生成唯一id
 *
 * @author errorrik (errorrik@gmail.com)
 */


    var idStart = 0x0907;

    module.exports = function () {
        return idStart++;
    };



/***/ }),
/* 37 */
/***/ (function(module, exports) {

/**
 * 事件扩展
 * @module zrender/mixin/Eventful
 * @author Kener (@Kener-林峰, kener.linfeng@gmail.com)
 *         pissang (https://www.github.com/pissang)
 */


    var arrySlice = Array.prototype.slice;

    /**
     * 事件分发器
     * @alias module:zrender/mixin/Eventful
     * @constructor
     */
    var Eventful = function () {
        this._$handlers = {};
    };

    Eventful.prototype = {

        constructor: Eventful,

        /**
         * 单次触发绑定，trigger后销毁
         *
         * @param {string} event 事件名
         * @param {Function} handler 响应函数
         * @param {Object} context
         */
        one: function (event, handler, context) {
            var _h = this._$handlers;

            if (!handler || !event) {
                return this;
            }

            if (!_h[event]) {
                _h[event] = [];
            }

            for (var i = 0; i < _h[event].length; i++) {
                if (_h[event][i].h === handler) {
                    return this;
                }
            }

            _h[event].push({
                h: handler,
                one: true,
                ctx: context || this
            });

            return this;
        },

        /**
         * 绑定事件
         * @param {string} event 事件名
         * @param {Function} handler 事件处理函数
         * @param {Object} [context]
         */
        on: function (event, handler, context) {
            var _h = this._$handlers;

            if (!handler || !event) {
                return this;
            }

            if (!_h[event]) {
                _h[event] = [];
            }

            for (var i = 0; i < _h[event].length; i++) {
                if (_h[event][i].h === handler) {
                    return this;
                }
            }

            _h[event].push({
                h: handler,
                one: false,
                ctx: context || this
            });

            return this;
        },

        /**
         * 是否绑定了事件
         * @param  {string}  event
         * @return {boolean}
         */
        isSilent: function (event) {
            var _h = this._$handlers;
            return _h[event] && _h[event].length;
        },

        /**
         * 解绑事件
         * @param {string} event 事件名
         * @param {Function} [handler] 事件处理函数
         */
        off: function (event, handler) {
            var _h = this._$handlers;

            if (!event) {
                this._$handlers = {};
                return this;
            }

            if (handler) {
                if (_h[event]) {
                    var newList = [];
                    for (var i = 0, l = _h[event].length; i < l; i++) {
                        if (_h[event][i]['h'] != handler) {
                            newList.push(_h[event][i]);
                        }
                    }
                    _h[event] = newList;
                }

                if (_h[event] && _h[event].length === 0) {
                    delete _h[event];
                }
            }
            else {
                delete _h[event];
            }

            return this;
        },

        /**
         * 事件分发
         *
         * @param {string} type 事件类型
         */
        trigger: function (type) {
            if (this._$handlers[type]) {
                var args = arguments;
                var argLen = args.length;

                if (argLen > 3) {
                    args = arrySlice.call(args, 1);
                }

                var _h = this._$handlers[type];
                var len = _h.length;
                for (var i = 0; i < len;) {
                    // Optimize advise from backbone
                    switch (argLen) {
                        case 1:
                            _h[i]['h'].call(_h[i]['ctx']);
                            break;
                        case 2:
                            _h[i]['h'].call(_h[i]['ctx'], args[1]);
                            break;
                        case 3:
                            _h[i]['h'].call(_h[i]['ctx'], args[1], args[2]);
                            break;
                        default:
                            // have more than 2 given arguments
                            _h[i]['h'].apply(_h[i]['ctx'], args);
                            break;
                    }

                    if (_h[i]['one']) {
                        _h.splice(i, 1);
                        len--;
                    }
                    else {
                        i++;
                    }
                }
            }

            return this;
        },

        /**
         * 带有context的事件分发, 最后一个参数是事件回调的context
         * @param {string} type 事件类型
         */
        triggerWithContext: function (type) {
            if (this._$handlers[type]) {
                var args = arguments;
                var argLen = args.length;

                if (argLen > 4) {
                    args = arrySlice.call(args, 1, args.length - 1);
                }
                var ctx = args[args.length - 1];

                var _h = this._$handlers[type];
                var len = _h.length;
                for (var i = 0; i < len;) {
                    // Optimize advise from backbone
                    switch (argLen) {
                        case 1:
                            _h[i]['h'].call(ctx);
                            break;
                        case 2:
                            _h[i]['h'].call(ctx, args[1]);
                            break;
                        case 3:
                            _h[i]['h'].call(ctx, args[1], args[2]);
                            break;
                        default:
                            // have more than 2 given arguments
                            _h[i]['h'].apply(ctx, args);
                            break;
                    }

                    if (_h[i]['one']) {
                        _h.splice(i, 1);
                        len--;
                    }
                    else {
                        i++;
                    }
                }
            }

            return this;
        }
    };

    // 对象可以通过 onxxxx 绑定事件
    /**
     * @event module:zrender/mixin/Eventful#onclick
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmouseover
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmouseout
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmousemove
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmousewheel
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmousedown
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#onmouseup
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondrag
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondragstart
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondragend
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondragenter
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondragleave
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondragover
     * @type {Function}
     * @default null
     */
    /**
     * @event module:zrender/mixin/Eventful#ondrop
     * @type {Function}
     * @default null
     */

    module.exports = Eventful;



/***/ }),
/* 38 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * @module zrender/mixin/Animatable
 */


    var Animator = __webpack_require__(39);
    var util = __webpack_require__(0);
    var isString = util.isString;
    var isFunction = util.isFunction;
    var isObject = util.isObject;
    var log = __webpack_require__(42);

    /**
     * @alias modue:zrender/mixin/Animatable
     * @constructor
     */
    var Animatable = function () {

        /**
         * @type {Array.<module:zrender/animation/Animator>}
         * @readOnly
         */
        this.animators = [];
    };

    Animatable.prototype = {

        constructor: Animatable,

        /**
         * 动画
         *
         * @param {string} path 需要添加动画的属性获取路径，可以通过a.b.c来获取深层的属性
         * @param {boolean} [loop] 动画是否循环
         * @return {module:zrender/animation/Animator}
         * @example:
         *     el.animate('style', false)
         *         .when(1000, {x: 10} )
         *         .done(function(){ // Animation done })
         *         .start()
         */
        animate: function (path, loop) {
            var target;
            var animatingShape = false;
            var el = this;
            var zr = this.__zr;
            if (path) {
                var pathSplitted = path.split('.');
                var prop = el;
                // If animating shape
                animatingShape = pathSplitted[0] === 'shape';
                for (var i = 0, l = pathSplitted.length; i < l; i++) {
                    if (!prop) {
                        continue;
                    }
                    prop = prop[pathSplitted[i]];
                }
                if (prop) {
                    target = prop;
                }
            }
            else {
                target = el;
            }

            if (!target) {
                log(
                    'Property "'
                    + path
                    + '" is not existed in element '
                    + el.id
                );
                return;
            }

            var animators = el.animators;

            var animator = new Animator(target, loop);

            animator.during(function (target) {
                el.dirty(animatingShape);
            })
            .done(function () {
                // FIXME Animator will not be removed if use `Animator#stop` to stop animation
                animators.splice(util.indexOf(animators, animator), 1);
            });

            animators.push(animator);

            // If animate after added to the zrender
            if (zr) {
                zr.animation.addAnimator(animator);
            }

            return animator;
        },

        /**
         * 停止动画
         * @param {boolean} forwardToLast If move to last frame before stop
         */
        stopAnimation: function (forwardToLast) {
            var animators = this.animators;
            var len = animators.length;
            for (var i = 0; i < len; i++) {
                animators[i].stop(forwardToLast);
            }
            animators.length = 0;

            return this;
        },

        /**
         * @param {Object} target
         * @param {number} [time=500] Time in ms
         * @param {string} [easing='linear']
         * @param {number} [delay=0]
         * @param {Function} [callback]
         *
         * @example
         *  // Animate position
         *  el.animateTo({
         *      position: [10, 10]
         *  }, function () { // done })
         *
         *  // Animate shape, style and position in 100ms, delayed 100ms, with cubicOut easing
         *  el.animateTo({
         *      shape: {
         *          width: 500
         *      },
         *      style: {
         *          fill: 'red'
         *      }
         *      position: [10, 10]
         *  }, 100, 100, 'cubicOut', function () { // done })
         */
         // TODO Return animation key
        animateTo: function (target, time, delay, easing, callback) {
            // animateTo(target, time, easing, callback);
            if (isString(delay)) {
                callback = easing;
                easing = delay;
                delay = 0;
            }
            // animateTo(target, time, delay, callback);
            else if (isFunction(easing)) {
                callback = easing;
                easing = 'linear';
                delay = 0;
            }
            // animateTo(target, time, callback);
            else if (isFunction(delay)) {
                callback = delay;
                delay = 0;
            }
            // animateTo(target, callback)
            else if (isFunction(time)) {
                callback = time;
                time = 500;
            }
            // animateTo(target)
            else if (!time) {
                time = 500;
            }
            // Stop all previous animations
            this.stopAnimation();
            this._animateToShallow('', this, target, time, delay, easing, callback);

            // Animators may be removed immediately after start
            // if there is nothing to animate
            var animators = this.animators.slice();
            var count = animators.length;
            function done() {
                count--;
                if (!count) {
                    callback && callback();
                }
            }

            // No animators. This should be checked before animators[i].start(),
            // because 'done' may be executed immediately if no need to animate.
            if (!count) {
                callback && callback();
            }
            // Start after all animators created
            // Incase any animator is done immediately when all animation properties are not changed
            for (var i = 0; i < animators.length; i++) {
                animators[i]
                    .done(done)
                    .start(easing);
            }
        },

        /**
         * @private
         * @param {string} path=''
         * @param {Object} source=this
         * @param {Object} target
         * @param {number} [time=500]
         * @param {number} [delay=0]
         *
         * @example
         *  // Animate position
         *  el._animateToShallow({
         *      position: [10, 10]
         *  })
         *
         *  // Animate shape, style and position in 100ms, delayed 100ms
         *  el._animateToShallow({
         *      shape: {
         *          width: 500
         *      },
         *      style: {
         *          fill: 'red'
         *      }
         *      position: [10, 10]
         *  }, 100, 100)
         */
        _animateToShallow: function (path, source, target, time, delay) {
            var objShallow = {};
            var propertyCount = 0;
            for (var name in target) {
                if (!target.hasOwnProperty(name)) {
                    continue;
                }

                if (source[name] != null) {
                    if (isObject(target[name]) && !util.isArrayLike(target[name])) {
                        this._animateToShallow(
                            path ? path + '.' + name : name,
                            source[name],
                            target[name],
                            time,
                            delay
                        );
                    }
                    else {
                        objShallow[name] = target[name];
                        propertyCount++;
                    }
                }
                else if (target[name] != null) {
                    // Attr directly if not has property
                    // FIXME, if some property not needed for element ?
                    if (!path) {
                        this.attr(name, target[name]);
                    }
                    else {  // Shape or style
                        var props = {};
                        props[path] = {};
                        props[path][name] = target[name];
                        this.attr(props);
                    }
                }
            }

            if (propertyCount > 0) {
                this.animate(path, false)
                    .when(time == null ? 500 : time, objShallow)
                    .delay(delay || 0);
            }

            return this;
        }
    };

    module.exports = Animatable;


/***/ }),
/* 39 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * @module echarts/animation/Animator
 */


    var Clip = __webpack_require__(40);
    var color = __webpack_require__(16);
    var util = __webpack_require__(0);
    var isArrayLike = util.isArrayLike;

    var arraySlice = Array.prototype.slice;

    function defaultGetter(target, key) {
        return target[key];
    }

    function defaultSetter(target, key, value) {
        target[key] = value;
    }

    /**
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} percent
     * @return {number}
     */
    function interpolateNumber(p0, p1, percent) {
        return (p1 - p0) * percent + p0;
    }

    /**
     * @param  {string} p0
     * @param  {string} p1
     * @param  {number} percent
     * @return {string}
     */
    function interpolateString(p0, p1, percent) {
        return percent > 0.5 ? p1 : p0;
    }

    /**
     * @param  {Array} p0
     * @param  {Array} p1
     * @param  {number} percent
     * @param  {Array} out
     * @param  {number} arrDim
     */
    function interpolateArray(p0, p1, percent, out, arrDim) {
        var len = p0.length;
        if (arrDim == 1) {
            for (var i = 0; i < len; i++) {
                out[i] = interpolateNumber(p0[i], p1[i], percent);
            }
        }
        else {
            var len2 = len && p0[0].length;
            for (var i = 0; i < len; i++) {
                for (var j = 0; j < len2; j++) {
                    out[i][j] = interpolateNumber(
                        p0[i][j], p1[i][j], percent
                    );
                }
            }
        }
    }

    // arr0 is source array, arr1 is target array.
    // Do some preprocess to avoid error happened when interpolating from arr0 to arr1
    function fillArr(arr0, arr1, arrDim) {
        var arr0Len = arr0.length;
        var arr1Len = arr1.length;
        if (arr0Len !== arr1Len) {
            // FIXME Not work for TypedArray
            var isPreviousLarger = arr0Len > arr1Len;
            if (isPreviousLarger) {
                // Cut the previous
                arr0.length = arr1Len;
            }
            else {
                // Fill the previous
                for (var i = arr0Len; i < arr1Len; i++) {
                    arr0.push(
                        arrDim === 1 ? arr1[i] : arraySlice.call(arr1[i])
                    );
                }
            }
        }
        // Handling NaN value
        var len2 = arr0[0] && arr0[0].length;
        for (var i = 0; i < arr0.length; i++) {
            if (arrDim === 1) {
                if (isNaN(arr0[i])) {
                    arr0[i] = arr1[i];
                }
            }
            else {
                for (var j = 0; j < len2; j++) {
                    if (isNaN(arr0[i][j])) {
                        arr0[i][j] = arr1[i][j];
                    }
                }
            }
        }
    }

    /**
     * @param  {Array} arr0
     * @param  {Array} arr1
     * @param  {number} arrDim
     * @return {boolean}
     */
    function isArraySame(arr0, arr1, arrDim) {
        if (arr0 === arr1) {
            return true;
        }
        var len = arr0.length;
        if (len !== arr1.length) {
            return false;
        }
        if (arrDim === 1) {
            for (var i = 0; i < len; i++) {
                if (arr0[i] !== arr1[i]) {
                    return false;
                }
            }
        }
        else {
            var len2 = arr0[0].length;
            for (var i = 0; i < len; i++) {
                for (var j = 0; j < len2; j++) {
                    if (arr0[i][j] !== arr1[i][j]) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Catmull Rom interpolate array
     * @param  {Array} p0
     * @param  {Array} p1
     * @param  {Array} p2
     * @param  {Array} p3
     * @param  {number} t
     * @param  {number} t2
     * @param  {number} t3
     * @param  {Array} out
     * @param  {number} arrDim
     */
    function catmullRomInterpolateArray(
        p0, p1, p2, p3, t, t2, t3, out, arrDim
    ) {
        var len = p0.length;
        if (arrDim == 1) {
            for (var i = 0; i < len; i++) {
                out[i] = catmullRomInterpolate(
                    p0[i], p1[i], p2[i], p3[i], t, t2, t3
                );
            }
        }
        else {
            var len2 = p0[0].length;
            for (var i = 0; i < len; i++) {
                for (var j = 0; j < len2; j++) {
                    out[i][j] = catmullRomInterpolate(
                        p0[i][j], p1[i][j], p2[i][j], p3[i][j],
                        t, t2, t3
                    );
                }
            }
        }
    }

    /**
     * Catmull Rom interpolate number
     * @param  {number} p0
     * @param  {number} p1
     * @param  {number} p2
     * @param  {number} p3
     * @param  {number} t
     * @param  {number} t2
     * @param  {number} t3
     * @return {number}
     */
    function catmullRomInterpolate(p0, p1, p2, p3, t, t2, t3) {
        var v0 = (p2 - p0) * 0.5;
        var v1 = (p3 - p1) * 0.5;
        return (2 * (p1 - p2) + v0 + v1) * t3
                + (-3 * (p1 - p2) - 2 * v0 - v1) * t2
                + v0 * t + p1;
    }

    function cloneValue(value) {
        if (isArrayLike(value)) {
            var len = value.length;
            if (isArrayLike(value[0])) {
                var ret = [];
                for (var i = 0; i < len; i++) {
                    ret.push(arraySlice.call(value[i]));
                }
                return ret;
            }

            return arraySlice.call(value);
        }

        return value;
    }

    function rgba2String(rgba) {
        rgba[0] = Math.floor(rgba[0]);
        rgba[1] = Math.floor(rgba[1]);
        rgba[2] = Math.floor(rgba[2]);

        return 'rgba(' + rgba.join(',') + ')';
    }

    function getArrayDim(keyframes) {
        var lastValue = keyframes[keyframes.length - 1].value;
        return isArrayLike(lastValue && lastValue[0]) ? 2 : 1;
    }

    function createTrackClip (animator, easing, oneTrackDone, keyframes, propName) {
        var getter = animator._getter;
        var setter = animator._setter;
        var useSpline = easing === 'spline';

        var trackLen = keyframes.length;
        if (!trackLen) {
            return;
        }
        // Guess data type
        var firstVal = keyframes[0].value;
        var isValueArray = isArrayLike(firstVal);
        var isValueColor = false;
        var isValueString = false;

        // For vertices morphing
        var arrDim = isValueArray ? getArrayDim(keyframes) : 0;

        var trackMaxTime;
        // Sort keyframe as ascending
        keyframes.sort(function(a, b) {
            return a.time - b.time;
        });

        trackMaxTime = keyframes[trackLen - 1].time;
        // Percents of each keyframe
        var kfPercents = [];
        // Value of each keyframe
        var kfValues = [];
        var prevValue = keyframes[0].value;
        var isAllValueEqual = true;
        for (var i = 0; i < trackLen; i++) {
            kfPercents.push(keyframes[i].time / trackMaxTime);
            // Assume value is a color when it is a string
            var value = keyframes[i].value;

            // Check if value is equal, deep check if value is array
            if (!((isValueArray && isArraySame(value, prevValue, arrDim))
                || (!isValueArray && value === prevValue))) {
                isAllValueEqual = false;
            }
            prevValue = value;

            // Try converting a string to a color array
            if (typeof value == 'string') {
                var colorArray = color.parse(value);
                if (colorArray) {
                    value = colorArray;
                    isValueColor = true;
                }
                else {
                    isValueString = true;
                }
            }
            kfValues.push(value);
        }
        if (isAllValueEqual) {
            return;
        }

        var lastValue = kfValues[trackLen - 1];
        // Polyfill array and NaN value
        for (var i = 0; i < trackLen - 1; i++) {
            if (isValueArray) {
                fillArr(kfValues[i], lastValue, arrDim);
            }
            else {
                if (isNaN(kfValues[i]) && !isNaN(lastValue) && !isValueString && !isValueColor) {
                    kfValues[i] = lastValue;
                }
            }
        }
        isValueArray && fillArr(getter(animator._target, propName), lastValue, arrDim);

        // Cache the key of last frame to speed up when
        // animation playback is sequency
        var lastFrame = 0;
        var lastFramePercent = 0;
        var start;
        var w;
        var p0;
        var p1;
        var p2;
        var p3;

        if (isValueColor) {
            var rgba = [0, 0, 0, 0];
        }

        var onframe = function (target, percent) {
            // Find the range keyframes
            // kf1-----kf2---------current--------kf3
            // find kf2 and kf3 and do interpolation
            var frame;
            // In the easing function like elasticOut, percent may less than 0
            if (percent < 0) {
                frame = 0;
            }
            else if (percent < lastFramePercent) {
                // Start from next key
                // PENDING start from lastFrame ?
                start = Math.min(lastFrame + 1, trackLen - 1);
                for (frame = start; frame >= 0; frame--) {
                    if (kfPercents[frame] <= percent) {
                        break;
                    }
                }
                // PENDING really need to do this ?
                frame = Math.min(frame, trackLen - 2);
            }
            else {
                for (frame = lastFrame; frame < trackLen; frame++) {
                    if (kfPercents[frame] > percent) {
                        break;
                    }
                }
                frame = Math.min(frame - 1, trackLen - 2);
            }
            lastFrame = frame;
            lastFramePercent = percent;

            var range = (kfPercents[frame + 1] - kfPercents[frame]);
            if (range === 0) {
                return;
            }
            else {
                w = (percent - kfPercents[frame]) / range;
            }
            if (useSpline) {
                p1 = kfValues[frame];
                p0 = kfValues[frame === 0 ? frame : frame - 1];
                p2 = kfValues[frame > trackLen - 2 ? trackLen - 1 : frame + 1];
                p3 = kfValues[frame > trackLen - 3 ? trackLen - 1 : frame + 2];
                if (isValueArray) {
                    catmullRomInterpolateArray(
                        p0, p1, p2, p3, w, w * w, w * w * w,
                        getter(target, propName),
                        arrDim
                    );
                }
                else {
                    var value;
                    if (isValueColor) {
                        value = catmullRomInterpolateArray(
                            p0, p1, p2, p3, w, w * w, w * w * w,
                            rgba, 1
                        );
                        value = rgba2String(rgba);
                    }
                    else if (isValueString) {
                        // String is step(0.5)
                        return interpolateString(p1, p2, w);
                    }
                    else {
                        value = catmullRomInterpolate(
                            p0, p1, p2, p3, w, w * w, w * w * w
                        );
                    }
                    setter(
                        target,
                        propName,
                        value
                    );
                }
            }
            else {
                if (isValueArray) {
                    interpolateArray(
                        kfValues[frame], kfValues[frame + 1], w,
                        getter(target, propName),
                        arrDim
                    );
                }
                else {
                    var value;
                    if (isValueColor) {
                        interpolateArray(
                            kfValues[frame], kfValues[frame + 1], w,
                            rgba, 1
                        );
                        value = rgba2String(rgba);
                    }
                    else if (isValueString) {
                        // String is step(0.5)
                        return interpolateString(kfValues[frame], kfValues[frame + 1], w);
                    }
                    else {
                        value = interpolateNumber(kfValues[frame], kfValues[frame + 1], w);
                    }
                    setter(
                        target,
                        propName,
                        value
                    );
                }
            }
        };

        var clip = new Clip({
            target: animator._target,
            life: trackMaxTime,
            loop: animator._loop,
            delay: animator._delay,
            onframe: onframe,
            ondestroy: oneTrackDone
        });

        if (easing && easing !== 'spline') {
            clip.easing = easing;
        }

        return clip;
    }

    /**
     * @alias module:zrender/animation/Animator
     * @constructor
     * @param {Object} target
     * @param {boolean} loop
     * @param {Function} getter
     * @param {Function} setter
     */
    var Animator = function(target, loop, getter, setter) {
        this._tracks = {};
        this._target = target;

        this._loop = loop || false;

        this._getter = getter || defaultGetter;
        this._setter = setter || defaultSetter;

        this._clipCount = 0;

        this._delay = 0;

        this._doneList = [];

        this._onframeList = [];

        this._clipList = [];
    };

    Animator.prototype = {
        /**
         * 设置动画关键帧
         * @param  {number} time 关键帧时间，单位是ms
         * @param  {Object} props 关键帧的属性值，key-value表示
         * @return {module:zrender/animation/Animator}
         */
        when: function(time /* ms */, props) {
            var tracks = this._tracks;
            for (var propName in props) {
                if (!props.hasOwnProperty(propName)) {
                    continue;
                }

                if (!tracks[propName]) {
                    tracks[propName] = [];
                    // Invalid value
                    var value = this._getter(this._target, propName);
                    if (value == null) {
                        // zrLog('Invalid property ' + propName);
                        continue;
                    }
                    // If time is 0
                    //  Then props is given initialize value
                    // Else
                    //  Initialize value from current prop value
                    if (time !== 0) {
                        tracks[propName].push({
                            time: 0,
                            value: cloneValue(value)
                        });
                    }
                }
                tracks[propName].push({
                    time: time,
                    value: props[propName]
                });
            }
            return this;
        },
        /**
         * 添加动画每一帧的回调函数
         * @param  {Function} callback
         * @return {module:zrender/animation/Animator}
         */
        during: function (callback) {
            this._onframeList.push(callback);
            return this;
        },

        pause: function () {
            for (var i = 0; i < this._clipList.length; i++) {
                this._clipList[i].pause();
            }
            this._paused = true;
        },

        resume: function () {
            for (var i = 0; i < this._clipList.length; i++) {
                this._clipList[i].resume();
            }
            this._paused = false;
        },

        isPaused: function () {
            return !!this._paused;
        },

        _doneCallback: function () {
            // Clear all tracks
            this._tracks = {};
            // Clear all clips
            this._clipList.length = 0;

            var doneList = this._doneList;
            var len = doneList.length;
            for (var i = 0; i < len; i++) {
                doneList[i].call(this);
            }
        },
        /**
         * 开始执行动画
         * @param  {string|Function} easing
         *         动画缓动函数，详见{@link module:zrender/animation/easing}
         * @return {module:zrender/animation/Animator}
         */
        start: function (easing) {

            var self = this;
            var clipCount = 0;

            var oneTrackDone = function() {
                clipCount--;
                if (!clipCount) {
                    self._doneCallback();
                }
            };

            var lastClip;
            for (var propName in this._tracks) {
                if (!this._tracks.hasOwnProperty(propName)) {
                    continue;
                }
                var clip = createTrackClip(
                    this, easing, oneTrackDone,
                    this._tracks[propName], propName
                );
                if (clip) {
                    this._clipList.push(clip);
                    clipCount++;

                    // If start after added to animation
                    if (this.animation) {
                        this.animation.addClip(clip);
                    }

                    lastClip = clip;
                }
            }

            // Add during callback on the last clip
            if (lastClip) {
                var oldOnFrame = lastClip.onframe;
                lastClip.onframe = function (target, percent) {
                    oldOnFrame(target, percent);

                    for (var i = 0; i < self._onframeList.length; i++) {
                        self._onframeList[i](target, percent);
                    }
                };
            }

            if (!clipCount) {
                this._doneCallback();
            }
            return this;
        },
        /**
         * 停止动画
         * @param {boolean} forwardToLast If move to last frame before stop
         */
        stop: function (forwardToLast) {
            var clipList = this._clipList;
            var animation = this.animation;
            for (var i = 0; i < clipList.length; i++) {
                var clip = clipList[i];
                if (forwardToLast) {
                    // Move to last frame before stop
                    clip.onframe(this._target, 1);
                }
                animation && animation.removeClip(clip);
            }
            clipList.length = 0;
        },
        /**
         * 设置动画延迟开始的时间
         * @param  {number} time 单位ms
         * @return {module:zrender/animation/Animator}
         */
        delay: function (time) {
            this._delay = time;
            return this;
        },
        /**
         * 添加动画结束的回调
         * @param  {Function} cb
         * @return {module:zrender/animation/Animator}
         */
        done: function(cb) {
            if (cb) {
                this._doneList.push(cb);
            }
            return this;
        },

        /**
         * @return {Array.<module:zrender/animation/Clip>}
         */
        getClips: function () {
            return this._clipList;
        }
    };

    module.exports = Animator;


/***/ }),
/* 40 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 动画主控制器
 * @config target 动画对象，可以是数组，如果是数组的话会批量分发onframe等事件
 * @config life(1000) 动画时长
 * @config delay(0) 动画延迟时间
 * @config loop(true)
 * @config gap(0) 循环的间隔时间
 * @config onframe
 * @config easing(optional)
 * @config ondestroy(optional)
 * @config onrestart(optional)
 *
 * TODO pause
 */


    var easingFuncs = __webpack_require__(41);

    function Clip(options) {

        this._target = options.target;

        // 生命周期
        this._life = options.life || 1000;
        // 延时
        this._delay = options.delay || 0;
        // 开始时间
        // this._startTime = new Date().getTime() + this._delay;// 单位毫秒
        this._initialized = false;

        // 是否循环
        this.loop = options.loop == null ? false : options.loop;

        this.gap = options.gap || 0;

        this.easing = options.easing || 'Linear';

        this.onframe = options.onframe;
        this.ondestroy = options.ondestroy;
        this.onrestart = options.onrestart;

        this._pausedTime = 0;
        this._paused = false;
    }

    Clip.prototype = {

        constructor: Clip,

        step: function (globalTime, deltaTime) {
            // Set startTime on first step, or _startTime may has milleseconds different between clips
            // PENDING
            if (!this._initialized) {
                this._startTime = globalTime + this._delay;
                this._initialized = true;
            }

            if (this._paused) {
                this._pausedTime += deltaTime;
                return;
            }

            var percent = (globalTime - this._startTime - this._pausedTime) / this._life;

            // 还没开始
            if (percent < 0) {
                return;
            }

            percent = Math.min(percent, 1);

            var easing = this.easing;
            var easingFunc = typeof easing == 'string' ? easingFuncs[easing] : easing;
            var schedule = typeof easingFunc === 'function'
                ? easingFunc(percent)
                : percent;

            this.fire('frame', schedule);

            // 结束
            if (percent == 1) {
                if (this.loop) {
                    this.restart (globalTime);
                    // 重新开始周期
                    // 抛出而不是直接调用事件直到 stage.update 后再统一调用这些事件
                    return 'restart';
                }

                // 动画完成将这个控制器标识为待删除
                // 在Animation.update中进行批量删除
                this._needsRemove = true;
                return 'destroy';
            }

            return null;
        },

        restart: function (globalTime) {
            var remainder = (globalTime - this._startTime - this._pausedTime) % this._life;
            this._startTime = globalTime - remainder + this.gap;
            this._pausedTime = 0;

            this._needsRemove = false;
        },

        fire: function (eventType, arg) {
            eventType = 'on' + eventType;
            if (this[eventType]) {
                this[eventType](this._target, arg);
            }
        },

        pause: function () {
            this._paused = true;
        },

        resume: function () {
            this._paused = false;
        }
    };

    module.exports = Clip;



/***/ }),
/* 41 */
/***/ (function(module, exports) {

/**
 * 缓动代码来自 https://github.com/sole/tween.js/blob/master/src/Tween.js
 * @see http://sole.github.io/tween.js/examples/03_graphs.html
 * @exports zrender/animation/easing
 */

    var easing = {
        /**
        * @param {number} k
        * @return {number}
        */
        linear: function (k) {
            return k;
        },

        /**
        * @param {number} k
        * @return {number}
        */
        quadraticIn: function (k) {
            return k * k;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quadraticOut: function (k) {
            return k * (2 - k);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quadraticInOut: function (k) {
            if ((k *= 2) < 1) {
                return 0.5 * k * k;
            }
            return -0.5 * (--k * (k - 2) - 1);
        },

        // 三次方的缓动（t^3）
        /**
        * @param {number} k
        * @return {number}
        */
        cubicIn: function (k) {
            return k * k * k;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        cubicOut: function (k) {
            return --k * k * k + 1;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        cubicInOut: function (k) {
            if ((k *= 2) < 1) {
                return 0.5 * k * k * k;
            }
            return 0.5 * ((k -= 2) * k * k + 2);
        },

        // 四次方的缓动（t^4）
        /**
        * @param {number} k
        * @return {number}
        */
        quarticIn: function (k) {
            return k * k * k * k;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quarticOut: function (k) {
            return 1 - (--k * k * k * k);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quarticInOut: function (k) {
            if ((k *= 2) < 1) {
                return 0.5 * k * k * k * k;
            }
            return -0.5 * ((k -= 2) * k * k * k - 2);
        },

        // 五次方的缓动（t^5）
        /**
        * @param {number} k
        * @return {number}
        */
        quinticIn: function (k) {
            return k * k * k * k * k;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quinticOut: function (k) {
            return --k * k * k * k * k + 1;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        quinticInOut: function (k) {
            if ((k *= 2) < 1) {
                return 0.5 * k * k * k * k * k;
            }
            return 0.5 * ((k -= 2) * k * k * k * k + 2);
        },

        // 正弦曲线的缓动（sin(t)）
        /**
        * @param {number} k
        * @return {number}
        */
        sinusoidalIn: function (k) {
            return 1 - Math.cos(k * Math.PI / 2);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        sinusoidalOut: function (k) {
            return Math.sin(k * Math.PI / 2);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        sinusoidalInOut: function (k) {
            return 0.5 * (1 - Math.cos(Math.PI * k));
        },

        // 指数曲线的缓动（2^t）
        /**
        * @param {number} k
        * @return {number}
        */
        exponentialIn: function (k) {
            return k === 0 ? 0 : Math.pow(1024, k - 1);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        exponentialOut: function (k) {
            return k === 1 ? 1 : 1 - Math.pow(2, -10 * k);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        exponentialInOut: function (k) {
            if (k === 0) {
                return 0;
            }
            if (k === 1) {
                return 1;
            }
            if ((k *= 2) < 1) {
                return 0.5 * Math.pow(1024, k - 1);
            }
            return 0.5 * (-Math.pow(2, -10 * (k - 1)) + 2);
        },

        // 圆形曲线的缓动（sqrt(1-t^2)）
        /**
        * @param {number} k
        * @return {number}
        */
        circularIn: function (k) {
            return 1 - Math.sqrt(1 - k * k);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        circularOut: function (k) {
            return Math.sqrt(1 - (--k * k));
        },
        /**
        * @param {number} k
        * @return {number}
        */
        circularInOut: function (k) {
            if ((k *= 2) < 1) {
                return -0.5 * (Math.sqrt(1 - k * k) - 1);
            }
            return 0.5 * (Math.sqrt(1 - (k -= 2) * k) + 1);
        },

        // 创建类似于弹簧在停止前来回振荡的动画
        /**
        * @param {number} k
        * @return {number}
        */
        elasticIn: function (k) {
            var s;
            var a = 0.1;
            var p = 0.4;
            if (k === 0) {
                return 0;
            }
            if (k === 1) {
                return 1;
            }
            if (!a || a < 1) {
                a = 1; s = p / 4;
            }
            else {
                s = p * Math.asin(1 / a) / (2 * Math.PI);
            }
            return -(a * Math.pow(2, 10 * (k -= 1)) *
                        Math.sin((k - s) * (2 * Math.PI) / p));
        },
        /**
        * @param {number} k
        * @return {number}
        */
        elasticOut: function (k) {
            var s;
            var a = 0.1;
            var p = 0.4;
            if (k === 0) {
                return 0;
            }
            if (k === 1) {
                return 1;
            }
            if (!a || a < 1) {
                a = 1; s = p / 4;
            }
            else {
                s = p * Math.asin(1 / a) / (2 * Math.PI);
            }
            return (a * Math.pow(2, -10 * k) *
                    Math.sin((k - s) * (2 * Math.PI) / p) + 1);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        elasticInOut: function (k) {
            var s;
            var a = 0.1;
            var p = 0.4;
            if (k === 0) {
                return 0;
            }
            if (k === 1) {
                return 1;
            }
            if (!a || a < 1) {
                a = 1; s = p / 4;
            }
            else {
                s = p * Math.asin(1 / a) / (2 * Math.PI);
            }
            if ((k *= 2) < 1) {
                return -0.5 * (a * Math.pow(2, 10 * (k -= 1))
                    * Math.sin((k - s) * (2 * Math.PI) / p));
            }
            return a * Math.pow(2, -10 * (k -= 1))
                    * Math.sin((k - s) * (2 * Math.PI) / p) * 0.5 + 1;

        },

        // 在某一动画开始沿指示的路径进行动画处理前稍稍收回该动画的移动
        /**
        * @param {number} k
        * @return {number}
        */
        backIn: function (k) {
            var s = 1.70158;
            return k * k * ((s + 1) * k - s);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        backOut: function (k) {
            var s = 1.70158;
            return --k * k * ((s + 1) * k + s) + 1;
        },
        /**
        * @param {number} k
        * @return {number}
        */
        backInOut: function (k) {
            var s = 1.70158 * 1.525;
            if ((k *= 2) < 1) {
                return 0.5 * (k * k * ((s + 1) * k - s));
            }
            return 0.5 * ((k -= 2) * k * ((s + 1) * k + s) + 2);
        },

        // 创建弹跳效果
        /**
        * @param {number} k
        * @return {number}
        */
        bounceIn: function (k) {
            return 1 - easing.bounceOut(1 - k);
        },
        /**
        * @param {number} k
        * @return {number}
        */
        bounceOut: function (k) {
            if (k < (1 / 2.75)) {
                return 7.5625 * k * k;
            }
            else if (k < (2 / 2.75)) {
                return 7.5625 * (k -= (1.5 / 2.75)) * k + 0.75;
            }
            else if (k < (2.5 / 2.75)) {
                return 7.5625 * (k -= (2.25 / 2.75)) * k + 0.9375;
            }
            else {
                return 7.5625 * (k -= (2.625 / 2.75)) * k + 0.984375;
            }
        },
        /**
        * @param {number} k
        * @return {number}
        */
        bounceInOut: function (k) {
            if (k < 0.5) {
                return easing.bounceIn(k * 2) * 0.5;
            }
            return easing.bounceOut(k * 2 - 1) * 0.5 + 0.5;
        }
    };

    module.exports = easing;




/***/ }),
/* 42 */
/***/ (function(module, exports, __webpack_require__) {


        var config = __webpack_require__(18);

        /**
         * @exports zrender/tool/log
         * @author Kener (@Kener-林峰, kener.linfeng@gmail.com)
         */
        module.exports = function() {
            if (config.debugMode === 0) {
                return;
            }
            else if (config.debugMode == 1) {
                for (var k in arguments) {
                    throw new Error(arguments[k]);
                }
            }
            else if (config.debugMode > 1) {
                for (var k in arguments) {
                    console.log(arguments[k]);
                }
            }
        };

        /* for debug
        return function(mes) {
            document.getElementById('wrong-message').innerHTML =
                mes + ' ' + (new Date() - 0)
                + '<br/>'
                + document.getElementById('wrong-message').innerHTML;
        };
        */
    


/***/ }),
/* 43 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Mixin for drawing text in a element bounding rect
 * @module zrender/mixin/RectText
 */



    var textContain = __webpack_require__(5);
    var BoundingRect = __webpack_require__(3);

    var tmpRect = new BoundingRect();

    var RectText = function () {};

    function parsePercent(value, maxValue) {
        if (typeof value === 'string') {
            if (value.lastIndexOf('%') >= 0) {
                return parseFloat(value) / 100 * maxValue;
            }
            return parseFloat(value);
        }
        return value;
    }

    RectText.prototype = {

        constructor: RectText,

        /**
         * Draw text in a rect with specified position.
         * @param  {CanvasRenderingContext} ctx
         * @param  {Object} rect Displayable rect
         * @return {Object} textRect Alternative precalculated text bounding rect
         */
        drawRectText: function (ctx, rect, textRect) {
            var style = this.style;
            var text = style.text;
            // Convert to string
            text != null && (text += '');
            if (!text) {
                return;
            }

            // FIXME
            ctx.save();

            var x;
            var y;
            var textPosition = style.textPosition;
            var textOffset = style.textOffset;
            var distance = style.textDistance;
            var align = style.textAlign;
            var font = style.textFont || style.font;
            var baseline = style.textBaseline;
            var verticalAlign = style.textVerticalAlign;
            rect = style.textPositionRect || rect;

            textRect = textRect || textContain.getBoundingRect(text, font, align, baseline);

            // Transform rect to view space
            var transform = this.transform;
            if (!style.textTransform) {
                if (transform) {
                    tmpRect.copy(rect);
                    tmpRect.applyTransform(transform);
                    rect = tmpRect;
                }
            }
            else {
                this.setTransform(ctx);
            }

            // Text position represented by coord
            if (textPosition instanceof Array) {
                // Percent
                x = rect.x + parsePercent(textPosition[0], rect.width);
                y = rect.y + parsePercent(textPosition[1], rect.height);
                align = align || 'left';
                baseline = baseline || 'top';

                if (verticalAlign) {
                    switch (verticalAlign) {
                        case 'middle':
                            y -= textRect.height / 2 - textRect.lineHeight / 2;
                            break;
                        case 'bottom':
                            y -= textRect.height - textRect.lineHeight / 2;
                            break;
                        default:
                            y += textRect.lineHeight / 2;
                    }
                    // Force bseline to be middle
                    baseline = 'middle';
                }
            }
            else {
                var res = textContain.adjustTextPositionOnRect(
                    textPosition, rect, textRect, distance
                );
                x = res.x;
                y = res.y;
                // Default align and baseline when has textPosition
                align = align || res.textAlign;
                baseline = baseline || res.textBaseline;
            }

            if (textOffset) {
                x += textOffset[0];
                y += textOffset[1];
            }

            // Use canvas default left textAlign. Giving invalid value will cause state not change
            ctx.textAlign = align || 'left';
            // Use canvas default alphabetic baseline
            ctx.textBaseline = baseline || 'alphabetic';

            var textFill = style.textFill;
            var textStroke = style.textStroke;
            textFill && (ctx.fillStyle = textFill);
            textStroke && (ctx.strokeStyle = textStroke);

            // TODO Invalid font
            ctx.font = font || '12px sans-serif';

            // Text shadow
            // Always set shadowBlur and shadowOffset to avoid leak from displayable
            ctx.shadowBlur = style.textShadowBlur;
            ctx.shadowColor = style.textShadowColor || 'transparent';
            ctx.shadowOffsetX = style.textShadowOffsetX;
            ctx.shadowOffsetY = style.textShadowOffsetY;

            var textLines = text.split('\n');

            if (style.textRotation) {
                transform && ctx.translate(transform[4], transform[5]);
                ctx.rotate(style.textRotation);
                transform && ctx.translate(-transform[4], -transform[5]);
            }

            for (var i = 0; i < textLines.length; i++) {
                // Fill after stroke so the outline will not cover the main part.
                textStroke && ctx.strokeText(textLines[i], x, y);
                textFill && ctx.fillText(textLines[i], x, y);
                y += textRect.lineHeight;
            }

            ctx.restore();
        }
    };

    module.exports = RectText;


/***/ }),
/* 44 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * @author Yi Shen(https://github.com/pissang)
 */


    var vec2 = __webpack_require__(2);
    var curve = __webpack_require__(4);

    var bbox = {};
    var mathMin = Math.min;
    var mathMax = Math.max;
    var mathSin = Math.sin;
    var mathCos = Math.cos;

    var start = vec2.create();
    var end = vec2.create();
    var extremity = vec2.create();

    var PI2 = Math.PI * 2;
    /**
     * 从顶点数组中计算出最小包围盒，写入`min`和`max`中
     * @module zrender/core/bbox
     * @param {Array<Object>} points 顶点数组
     * @param {number} min
     * @param {number} max
     */
    bbox.fromPoints = function(points, min, max) {
        if (points.length === 0) {
            return;
        }
        var p = points[0];
        var left = p[0];
        var right = p[0];
        var top = p[1];
        var bottom = p[1];
        var i;

        for (i = 1; i < points.length; i++) {
            p = points[i];
            left = mathMin(left, p[0]);
            right = mathMax(right, p[0]);
            top = mathMin(top, p[1]);
            bottom = mathMax(bottom, p[1]);
        }

        min[0] = left;
        min[1] = top;
        max[0] = right;
        max[1] = bottom;
    };

    /**
     * @memberOf module:zrender/core/bbox
     * @param {number} x0
     * @param {number} y0
     * @param {number} x1
     * @param {number} y1
     * @param {Array.<number>} min
     * @param {Array.<number>} max
     */
    bbox.fromLine = function (x0, y0, x1, y1, min, max) {
        min[0] = mathMin(x0, x1);
        min[1] = mathMin(y0, y1);
        max[0] = mathMax(x0, x1);
        max[1] = mathMax(y0, y1);
    };

    var xDim = [];
    var yDim = [];
    /**
     * 从三阶贝塞尔曲线(p0, p1, p2, p3)中计算出最小包围盒，写入`min`和`max`中
     * @memberOf module:zrender/core/bbox
     * @param {number} x0
     * @param {number} y0
     * @param {number} x1
     * @param {number} y1
     * @param {number} x2
     * @param {number} y2
     * @param {number} x3
     * @param {number} y3
     * @param {Array.<number>} min
     * @param {Array.<number>} max
     */
    bbox.fromCubic = function(
        x0, y0, x1, y1, x2, y2, x3, y3, min, max
    ) {
        var cubicExtrema = curve.cubicExtrema;
        var cubicAt = curve.cubicAt;
        var i;
        var n = cubicExtrema(x0, x1, x2, x3, xDim);
        min[0] = Infinity;
        min[1] = Infinity;
        max[0] = -Infinity;
        max[1] = -Infinity;

        for (i = 0; i < n; i++) {
            var x = cubicAt(x0, x1, x2, x3, xDim[i]);
            min[0] = mathMin(x, min[0]);
            max[0] = mathMax(x, max[0]);
        }
        n = cubicExtrema(y0, y1, y2, y3, yDim);
        for (i = 0; i < n; i++) {
            var y = cubicAt(y0, y1, y2, y3, yDim[i]);
            min[1] = mathMin(y, min[1]);
            max[1] = mathMax(y, max[1]);
        }

        min[0] = mathMin(x0, min[0]);
        max[0] = mathMax(x0, max[0]);
        min[0] = mathMin(x3, min[0]);
        max[0] = mathMax(x3, max[0]);

        min[1] = mathMin(y0, min[1]);
        max[1] = mathMax(y0, max[1]);
        min[1] = mathMin(y3, min[1]);
        max[1] = mathMax(y3, max[1]);
    };

    /**
     * 从二阶贝塞尔曲线(p0, p1, p2)中计算出最小包围盒，写入`min`和`max`中
     * @memberOf module:zrender/core/bbox
     * @param {number} x0
     * @param {number} y0
     * @param {number} x1
     * @param {number} y1
     * @param {number} x2
     * @param {number} y2
     * @param {Array.<number>} min
     * @param {Array.<number>} max
     */
    bbox.fromQuadratic = function(x0, y0, x1, y1, x2, y2, min, max) {
        var quadraticExtremum = curve.quadraticExtremum;
        var quadraticAt = curve.quadraticAt;
        // Find extremities, where derivative in x dim or y dim is zero
        var tx =
            mathMax(
                mathMin(quadraticExtremum(x0, x1, x2), 1), 0
            );
        var ty =
            mathMax(
                mathMin(quadraticExtremum(y0, y1, y2), 1), 0
            );

        var x = quadraticAt(x0, x1, x2, tx);
        var y = quadraticAt(y0, y1, y2, ty);

        min[0] = mathMin(x0, x2, x);
        min[1] = mathMin(y0, y2, y);
        max[0] = mathMax(x0, x2, x);
        max[1] = mathMax(y0, y2, y);
    };

    /**
     * 从圆弧中计算出最小包围盒，写入`min`和`max`中
     * @method
     * @memberOf module:zrender/core/bbox
     * @param {number} x
     * @param {number} y
     * @param {number} rx
     * @param {number} ry
     * @param {number} startAngle
     * @param {number} endAngle
     * @param {number} anticlockwise
     * @param {Array.<number>} min
     * @param {Array.<number>} max
     */
    bbox.fromArc = function (
        x, y, rx, ry, startAngle, endAngle, anticlockwise, min, max
    ) {
        var vec2Min = vec2.min;
        var vec2Max = vec2.max;

        var diff = Math.abs(startAngle - endAngle);


        if (diff % PI2 < 1e-4 && diff > 1e-4) {
            // Is a circle
            min[0] = x - rx;
            min[1] = y - ry;
            max[0] = x + rx;
            max[1] = y + ry;
            return;
        }

        start[0] = mathCos(startAngle) * rx + x;
        start[1] = mathSin(startAngle) * ry + y;

        end[0] = mathCos(endAngle) * rx + x;
        end[1] = mathSin(endAngle) * ry + y;

        vec2Min(min, start, end);
        vec2Max(max, start, end);

        // Thresh to [0, Math.PI * 2]
        startAngle = startAngle % (PI2);
        if (startAngle < 0) {
            startAngle = startAngle + PI2;
        }
        endAngle = endAngle % (PI2);
        if (endAngle < 0) {
            endAngle = endAngle + PI2;
        }

        if (startAngle > endAngle && !anticlockwise) {
            endAngle += PI2;
        }
        else if (startAngle < endAngle && anticlockwise) {
            startAngle += PI2;
        }
        if (anticlockwise) {
            var tmp = endAngle;
            endAngle = startAngle;
            startAngle = tmp;
        }

        // var number = 0;
        // var step = (anticlockwise ? -Math.PI : Math.PI) / 2;
        for (var angle = 0; angle < endAngle; angle += Math.PI / 2) {
            if (angle > startAngle) {
                extremity[0] = mathCos(angle) * rx + x;
                extremity[1] = mathSin(angle) * ry + y;

                vec2Min(min, extremity, min);
                vec2Max(max, extremity, max);
            }
        }
    };

    module.exports = bbox;



/***/ }),
/* 45 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";



    var CMD = __webpack_require__(6).CMD;
    var line = __webpack_require__(46);
    var cubic = __webpack_require__(47);
    var quadratic = __webpack_require__(48);
    var arc = __webpack_require__(49);
    var normalizeRadian = __webpack_require__(19).normalizeRadian;
    var curve = __webpack_require__(4);

    var windingLine = __webpack_require__(50);

    var containStroke = line.containStroke;

    var PI2 = Math.PI * 2;

    var EPSILON = 1e-4;

    function isAroundEqual(a, b) {
        return Math.abs(a - b) < EPSILON;
    }

    // 临时数组
    var roots = [-1, -1, -1];
    var extrema = [-1, -1];

    function swapExtrema() {
        var tmp = extrema[0];
        extrema[0] = extrema[1];
        extrema[1] = tmp;
    }

    function windingCubic(x0, y0, x1, y1, x2, y2, x3, y3, x, y) {
        // Quick reject
        if (
            (y > y0 && y > y1 && y > y2 && y > y3)
            || (y < y0 && y < y1 && y < y2 && y < y3)
        ) {
            return 0;
        }
        var nRoots = curve.cubicRootAt(y0, y1, y2, y3, y, roots);
        if (nRoots === 0) {
            return 0;
        }
        else {
            var w = 0;
            var nExtrema = -1;
            var y0_, y1_;
            for (var i = 0; i < nRoots; i++) {
                var t = roots[i];

                // Avoid winding error when intersection point is the connect point of two line of polygon
                var unit = (t === 0 || t === 1) ? 0.5 : 1;

                var x_ = curve.cubicAt(x0, x1, x2, x3, t);
                if (x_ < x) { // Quick reject
                    continue;
                }
                if (nExtrema < 0) {
                    nExtrema = curve.cubicExtrema(y0, y1, y2, y3, extrema);
                    if (extrema[1] < extrema[0] && nExtrema > 1) {
                        swapExtrema();
                    }
                    y0_ = curve.cubicAt(y0, y1, y2, y3, extrema[0]);
                    if (nExtrema > 1) {
                        y1_ = curve.cubicAt(y0, y1, y2, y3, extrema[1]);
                    }
                }
                if (nExtrema == 2) {
                    // 分成三段单调函数
                    if (t < extrema[0]) {
                        w += y0_ < y0 ? unit : -unit;
                    }
                    else if (t < extrema[1]) {
                        w += y1_ < y0_ ? unit : -unit;
                    }
                    else {
                        w += y3 < y1_ ? unit : -unit;
                    }
                }
                else {
                    // 分成两段单调函数
                    if (t < extrema[0]) {
                        w += y0_ < y0 ? unit : -unit;
                    }
                    else {
                        w += y3 < y0_ ? unit : -unit;
                    }
                }
            }
            return w;
        }
    }

    function windingQuadratic(x0, y0, x1, y1, x2, y2, x, y) {
        // Quick reject
        if (
            (y > y0 && y > y1 && y > y2)
            || (y < y0 && y < y1 && y < y2)
        ) {
            return 0;
        }
        var nRoots = curve.quadraticRootAt(y0, y1, y2, y, roots);
        if (nRoots === 0) {
            return 0;
        }
        else {
            var t = curve.quadraticExtremum(y0, y1, y2);
            if (t >= 0 && t <= 1) {
                var w = 0;
                var y_ = curve.quadraticAt(y0, y1, y2, t);
                for (var i = 0; i < nRoots; i++) {
                    // Remove one endpoint.
                    var unit = (roots[i] === 0 || roots[i] === 1) ? 0.5 : 1;

                    var x_ = curve.quadraticAt(x0, x1, x2, roots[i]);
                    if (x_ < x) {   // Quick reject
                        continue;
                    }
                    if (roots[i] < t) {
                        w += y_ < y0 ? unit : -unit;
                    }
                    else {
                        w += y2 < y_ ? unit : -unit;
                    }
                }
                return w;
            }
            else {
                // Remove one endpoint.
                var unit = (roots[0] === 0 || roots[0] === 1) ? 0.5 : 1;

                var x_ = curve.quadraticAt(x0, x1, x2, roots[0]);
                if (x_ < x) {   // Quick reject
                    return 0;
                }
                return y2 < y0 ? unit : -unit;
            }
        }
    }

    // TODO
    // Arc 旋转
    function windingArc(
        cx, cy, r, startAngle, endAngle, anticlockwise, x, y
    ) {
        y -= cy;
        if (y > r || y < -r) {
            return 0;
        }
        var tmp = Math.sqrt(r * r - y * y);
        roots[0] = -tmp;
        roots[1] = tmp;

        var diff = Math.abs(startAngle - endAngle);
        if (diff < 1e-4) {
            return 0;
        }
        if (diff % PI2 < 1e-4) {
            // Is a circle
            startAngle = 0;
            endAngle = PI2;
            var dir = anticlockwise ? 1 : -1;
            if (x >= roots[0] + cx && x <= roots[1] + cx) {
                return dir;
            } else {
                return 0;
            }
        }

        if (anticlockwise) {
            var tmp = startAngle;
            startAngle = normalizeRadian(endAngle);
            endAngle = normalizeRadian(tmp);
        }
        else {
            startAngle = normalizeRadian(startAngle);
            endAngle = normalizeRadian(endAngle);
        }
        if (startAngle > endAngle) {
            endAngle += PI2;
        }

        var w = 0;
        for (var i = 0; i < 2; i++) {
            var x_ = roots[i];
            if (x_ + cx > x) {
                var angle = Math.atan2(y, x_);
                var dir = anticlockwise ? 1 : -1;
                if (angle < 0) {
                    angle = PI2 + angle;
                }
                if (
                    (angle >= startAngle && angle <= endAngle)
                    || (angle + PI2 >= startAngle && angle + PI2 <= endAngle)
                ) {
                    if (angle > Math.PI / 2 && angle < Math.PI * 1.5) {
                        dir = -dir;
                    }
                    w += dir;
                }
            }
        }
        return w;
    }

    function containPath(data, lineWidth, isStroke, x, y) {
        var w = 0;
        var xi = 0;
        var yi = 0;
        var x0 = 0;
        var y0 = 0;

        for (var i = 0; i < data.length;) {
            var cmd = data[i++];
            // Begin a new subpath
            if (cmd === CMD.M && i > 1) {
                // Close previous subpath
                if (!isStroke) {
                    w += windingLine(xi, yi, x0, y0, x, y);
                }
                // 如果被任何一个 subpath 包含
                // if (w !== 0) {
                //     return true;
                // }
            }

            if (i == 1) {
                // 如果第一个命令是 L, C, Q
                // 则 previous point 同绘制命令的第一个 point
                //
                // 第一个命令为 Arc 的情况下会在后面特殊处理
                xi = data[i];
                yi = data[i + 1];

                x0 = xi;
                y0 = yi;
            }

            switch (cmd) {
                case CMD.M:
                    // moveTo 命令重新创建一个新的 subpath, 并且更新新的起点
                    // 在 closePath 的时候使用
                    x0 = data[i++];
                    y0 = data[i++];
                    xi = x0;
                    yi = y0;
                    break;
                case CMD.L:
                    if (isStroke) {
                        if (containStroke(xi, yi, data[i], data[i + 1], lineWidth, x, y)) {
                            return true;
                        }
                    }
                    else {
                        // NOTE 在第一个命令为 L, C, Q 的时候会计算出 NaN
                        w += windingLine(xi, yi, data[i], data[i + 1], x, y) || 0;
                    }
                    xi = data[i++];
                    yi = data[i++];
                    break;
                case CMD.C:
                    if (isStroke) {
                        if (cubic.containStroke(xi, yi,
                            data[i++], data[i++], data[i++], data[i++], data[i], data[i + 1],
                            lineWidth, x, y
                        )) {
                            return true;
                        }
                    }
                    else {
                        w += windingCubic(
                            xi, yi,
                            data[i++], data[i++], data[i++], data[i++], data[i], data[i + 1],
                            x, y
                        ) || 0;
                    }
                    xi = data[i++];
                    yi = data[i++];
                    break;
                case CMD.Q:
                    if (isStroke) {
                        if (quadratic.containStroke(xi, yi,
                            data[i++], data[i++], data[i], data[i + 1],
                            lineWidth, x, y
                        )) {
                            return true;
                        }
                    }
                    else {
                        w += windingQuadratic(
                            xi, yi,
                            data[i++], data[i++], data[i], data[i + 1],
                            x, y
                        ) || 0;
                    }
                    xi = data[i++];
                    yi = data[i++];
                    break;
                case CMD.A:
                    // TODO Arc 判断的开销比较大
                    var cx = data[i++];
                    var cy = data[i++];
                    var rx = data[i++];
                    var ry = data[i++];
                    var theta = data[i++];
                    var dTheta = data[i++];
                    // TODO Arc 旋转
                    var psi = data[i++];
                    var anticlockwise = 1 - data[i++];
                    var x1 = Math.cos(theta) * rx + cx;
                    var y1 = Math.sin(theta) * ry + cy;
                    // 不是直接使用 arc 命令
                    if (i > 1) {
                        w += windingLine(xi, yi, x1, y1, x, y);
                    }
                    else {
                        // 第一个命令起点还未定义
                        x0 = x1;
                        y0 = y1;
                    }
                    // zr 使用scale来模拟椭圆, 这里也对x做一定的缩放
                    var _x = (x - cx) * ry / rx + cx;
                    if (isStroke) {
                        if (arc.containStroke(
                            cx, cy, ry, theta, theta + dTheta, anticlockwise,
                            lineWidth, _x, y
                        )) {
                            return true;
                        }
                    }
                    else {
                        w += windingArc(
                            cx, cy, ry, theta, theta + dTheta, anticlockwise,
                            _x, y
                        );
                    }
                    xi = Math.cos(theta + dTheta) * rx + cx;
                    yi = Math.sin(theta + dTheta) * ry + cy;
                    break;
                case CMD.R:
                    x0 = xi = data[i++];
                    y0 = yi = data[i++];
                    var width = data[i++];
                    var height = data[i++];
                    var x1 = x0 + width;
                    var y1 = y0 + height;
                    if (isStroke) {
                        if (containStroke(x0, y0, x1, y0, lineWidth, x, y)
                          || containStroke(x1, y0, x1, y1, lineWidth, x, y)
                          || containStroke(x1, y1, x0, y1, lineWidth, x, y)
                          || containStroke(x0, y1, x0, y0, lineWidth, x, y)
                        ) {
                            return true;
                        }
                    }
                    else {
                        // FIXME Clockwise ?
                        w += windingLine(x1, y0, x1, y1, x, y);
                        w += windingLine(x0, y1, x0, y0, x, y);
                    }
                    break;
                case CMD.Z:
                    if (isStroke) {
                        if (containStroke(
                            xi, yi, x0, y0, lineWidth, x, y
                        )) {
                            return true;
                        }
                    }
                    else {
                        // Close a subpath
                        w += windingLine(xi, yi, x0, y0, x, y);
                        // 如果被任何一个 subpath 包含
                        // FIXME subpaths may overlap
                        // if (w !== 0) {
                        //     return true;
                        // }
                    }
                    xi = x0;
                    yi = y0;
                    break;
            }
        }
        if (!isStroke && !isAroundEqual(yi, y0)) {
            w += windingLine(xi, yi, x0, y0, x, y) || 0;
        }
        return w !== 0;
    }

    module.exports = {
        contain: function (pathData, x, y) {
            return containPath(pathData, 0, false, x, y);
        },

        containStroke: function (pathData, lineWidth, x, y) {
            return containPath(pathData, lineWidth, true, x, y);
        }
    };


/***/ }),
/* 46 */
/***/ (function(module, exports) {


    module.exports = {
        /**
         * 线段包含判断
         * @param  {number}  x0
         * @param  {number}  y0
         * @param  {number}  x1
         * @param  {number}  y1
         * @param  {number}  lineWidth
         * @param  {number}  x
         * @param  {number}  y
         * @return {boolean}
         */
        containStroke: function (x0, y0, x1, y1, lineWidth, x, y) {
            if (lineWidth === 0) {
                return false;
            }
            var _l = lineWidth;
            var _a = 0;
            var _b = x0;
            // Quick reject
            if (
                (y > y0 + _l && y > y1 + _l)
                || (y < y0 - _l && y < y1 - _l)
                || (x > x0 + _l && x > x1 + _l)
                || (x < x0 - _l && x < x1 - _l)
            ) {
                return false;
            }

            if (x0 !== x1) {
                _a = (y0 - y1) / (x0 - x1);
                _b = (x0 * y1 - x1 * y0) / (x0 - x1) ;
            }
            else {
                return Math.abs(x - x0) <= _l / 2;
            }
            var tmp = _a * x - y + _b;
            var _s = tmp * tmp / (_a * _a + 1);
            return _s <= _l / 2 * _l / 2;
        }
    };


/***/ }),
/* 47 */
/***/ (function(module, exports, __webpack_require__) {



    var curve = __webpack_require__(4);

    module.exports = {
        /**
         * 三次贝塞尔曲线描边包含判断
         * @param  {number}  x0
         * @param  {number}  y0
         * @param  {number}  x1
         * @param  {number}  y1
         * @param  {number}  x2
         * @param  {number}  y2
         * @param  {number}  x3
         * @param  {number}  y3
         * @param  {number}  lineWidth
         * @param  {number}  x
         * @param  {number}  y
         * @return {boolean}
         */
        containStroke: function(x0, y0, x1, y1, x2, y2, x3, y3, lineWidth, x, y) {
            if (lineWidth === 0) {
                return false;
            }
            var _l = lineWidth;
            // Quick reject
            if (
                (y > y0 + _l && y > y1 + _l && y > y2 + _l && y > y3 + _l)
                || (y < y0 - _l && y < y1 - _l && y < y2 - _l && y < y3 - _l)
                || (x > x0 + _l && x > x1 + _l && x > x2 + _l && x > x3 + _l)
                || (x < x0 - _l && x < x1 - _l && x < x2 - _l && x < x3 - _l)
            ) {
                return false;
            }
            var d = curve.cubicProjectPoint(
                x0, y0, x1, y1, x2, y2, x3, y3,
                x, y, null
            );
            return d <= _l / 2;
        }
    };


/***/ }),
/* 48 */
/***/ (function(module, exports, __webpack_require__) {



    var curve = __webpack_require__(4);

    module.exports = {
        /**
         * 二次贝塞尔曲线描边包含判断
         * @param  {number}  x0
         * @param  {number}  y0
         * @param  {number}  x1
         * @param  {number}  y1
         * @param  {number}  x2
         * @param  {number}  y2
         * @param  {number}  lineWidth
         * @param  {number}  x
         * @param  {number}  y
         * @return {boolean}
         */
        containStroke: function (x0, y0, x1, y1, x2, y2, lineWidth, x, y) {
            if (lineWidth === 0) {
                return false;
            }
            var _l = lineWidth;
            // Quick reject
            if (
                (y > y0 + _l && y > y1 + _l && y > y2 + _l)
                || (y < y0 - _l && y < y1 - _l && y < y2 - _l)
                || (x > x0 + _l && x > x1 + _l && x > x2 + _l)
                || (x < x0 - _l && x < x1 - _l && x < x2 - _l)
            ) {
                return false;
            }
            var d = curve.quadraticProjectPoint(
                x0, y0, x1, y1, x2, y2,
                x, y, null
            );
            return d <= _l / 2;
        }
    };


/***/ }),
/* 49 */
/***/ (function(module, exports, __webpack_require__) {



    var normalizeRadian = __webpack_require__(19).normalizeRadian;
    var PI2 = Math.PI * 2;

    module.exports = {
        /**
         * 圆弧描边包含判断
         * @param  {number}  cx
         * @param  {number}  cy
         * @param  {number}  r
         * @param  {number}  startAngle
         * @param  {number}  endAngle
         * @param  {boolean}  anticlockwise
         * @param  {number} lineWidth
         * @param  {number}  x
         * @param  {number}  y
         * @return {Boolean}
         */
        containStroke: function (
            cx, cy, r, startAngle, endAngle, anticlockwise,
            lineWidth, x, y
        ) {

            if (lineWidth === 0) {
                return false;
            }
            var _l = lineWidth;

            x -= cx;
            y -= cy;
            var d = Math.sqrt(x * x + y * y);

            if ((d - _l > r) || (d + _l < r)) {
                return false;
            }
            if (Math.abs(startAngle - endAngle) % PI2 < 1e-4) {
                // Is a circle
                return true;
            }
            if (anticlockwise) {
                var tmp = startAngle;
                startAngle = normalizeRadian(endAngle);
                endAngle = normalizeRadian(tmp);
            } else {
                startAngle = normalizeRadian(startAngle);
                endAngle = normalizeRadian(endAngle);
            }
            if (startAngle > endAngle) {
                endAngle += PI2;
            }

            var angle = Math.atan2(y, x);
            if (angle < 0) {
                angle += PI2;
            }
            return (angle >= startAngle && angle <= endAngle)
                || (angle + PI2 >= startAngle && angle + PI2 <= endAngle);
        }
    };


/***/ }),
/* 50 */
/***/ (function(module, exports) {


    module.exports = function windingLine(x0, y0, x1, y1, x, y) {
        if ((y > y0 && y > y1) || (y < y0 && y < y1)) {
            return 0;
        }
        // Ignore horizontal line
        if (y1 === y0) {
            return 0;
        }
        var dir = y1 < y0 ? 1 : -1;
        var t = (y - y0) / (y1 - y0);

        // Avoid winding error when intersection point is the connect point of two line of polygon
        if (t === 1 || t === 0) {
            dir = y1 < y0 ? 0.5 : -0.5;
        }

        var x_ = t * (x1 - x0) + x0;

        return x_ > x ? dir : 0;
    };


/***/ }),
/* 51 */
/***/ (function(module, exports) {



    var Pattern = function (image, repeat) {
        // Should do nothing more in this constructor. Because gradient can be
        // declard by `color: {image: ...}`, where this constructor will not be called.

        this.image = image;
        this.repeat = repeat;

        // Can be cloned
        this.type = 'pattern';
    };

    Pattern.prototype.getCanvasPattern = function (ctx) {
        return ctx.createPattern(this.image, this.repeat || 'repeat');
    };

    module.exports = Pattern;


/***/ }),
/* 52 */
/***/ (function(module, exports, __webpack_require__) {



    var CMD = __webpack_require__(6).CMD;
    var vec2 = __webpack_require__(2);
    var v2ApplyTransform = vec2.applyTransform;

    var points = [[], [], []];
    var mathSqrt = Math.sqrt;
    var mathAtan2 = Math.atan2;
    function transformPath(path, m) {
        var data = path.data;
        var cmd;
        var nPoint;
        var i;
        var j;
        var k;
        var p;

        var M = CMD.M;
        var C = CMD.C;
        var L = CMD.L;
        var R = CMD.R;
        var A = CMD.A;
        var Q = CMD.Q;

        for (i = 0, j = 0; i < data.length;) {
            cmd = data[i++];
            j = i;
            nPoint = 0;

            switch (cmd) {
                case M:
                    nPoint = 1;
                    break;
                case L:
                    nPoint = 1;
                    break;
                case C:
                    nPoint = 3;
                    break;
                case Q:
                    nPoint = 2;
                    break;
                case A:
                    var x = m[4];
                    var y = m[5];
                    var sx = mathSqrt(m[0] * m[0] + m[1] * m[1]);
                    var sy = mathSqrt(m[2] * m[2] + m[3] * m[3]);
                    var angle = mathAtan2(-m[1] / sy, m[0] / sx);
                    // cx
                    data[i] *= sx;
                    data[i++] += x;
                    // cy
                    data[i] *= sy;
                    data[i++] += y;
                    // Scale rx and ry
                    // FIXME Assume psi is 0 here
                    data[i++] *= sx;
                    data[i++] *= sy;

                    // Start angle
                    data[i++] += angle;
                    // end angle
                    data[i++] += angle;
                    // FIXME psi
                    i += 2;
                    j = i;
                    break;
                case R:
                    // x0, y0
                    p[0] = data[i++];
                    p[1] = data[i++];
                    v2ApplyTransform(p, p, m);
                    data[j++] = p[0];
                    data[j++] = p[1];
                    // x1, y1
                    p[0] += data[i++];
                    p[1] += data[i++];
                    v2ApplyTransform(p, p, m);
                    data[j++] = p[0];
                    data[j++] = p[1];
            }

            for (k = 0; k < nPoint; k++) {
                var p = points[k];
                p[0] = data[i++];
                p[1] = data[i++];

                v2ApplyTransform(p, p, m);
                // Write back
                data[j++] = p[0];
                data[j++] = p[1];
            }
        }
    }

    module.exports = transformPath;


/***/ }),
/* 53 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Group是一个容器，可以插入子节点，Group的变换也会被应用到子节点上
 * @module zrender/graphic/Group
 * @example
 *     var Group = require('zrender/lib/container/Group');
 *     var Circle = require('zrender/lib/graphic/shape/Circle');
 *     var g = new Group();
 *     g.position[0] = 100;
 *     g.position[1] = 100;
 *     g.add(new Circle({
 *         style: {
 *             x: 100,
 *             y: 100,
 *             r: 20,
 *         }
 *     }));
 *     zr.add(g);
 */


    var zrUtil = __webpack_require__(0);
    var Element = __webpack_require__(14);
    var BoundingRect = __webpack_require__(3);

    /**
     * @alias module:zrender/graphic/Group
     * @constructor
     * @extends module:zrender/mixin/Transformable
     * @extends module:zrender/mixin/Eventful
     */
    var Group = function (opts) {

        opts = opts || {};

        Element.call(this, opts);

        for (var key in opts) {
            if (opts.hasOwnProperty(key)) {
                this[key] = opts[key];
            }
        }

        this._children = [];

        this.__storage = null;

        this.__dirty = true;
    };

    Group.prototype = {

        constructor: Group,

        isGroup: true,

        /**
         * @type {string}
         */
        type: 'group',

        /**
         * 所有子孙元素是否响应鼠标事件
         * @name module:/zrender/container/Group#silent
         * @type {boolean}
         * @default false
         */
        silent: false,

        /**
         * @return {Array.<module:zrender/Element>}
         */
        children: function () {
            return this._children.slice();
        },

        /**
         * 获取指定 index 的儿子节点
         * @param  {number} idx
         * @return {module:zrender/Element}
         */
        childAt: function (idx) {
            return this._children[idx];
        },

        /**
         * 获取指定名字的儿子节点
         * @param  {string} name
         * @return {module:zrender/Element}
         */
        childOfName: function (name) {
            var children = this._children;
            for (var i = 0; i < children.length; i++) {
                if (children[i].name === name) {
                    return children[i];
                }
             }
        },

        /**
         * @return {number}
         */
        childCount: function () {
            return this._children.length;
        },

        /**
         * 添加子节点到最后
         * @param {module:zrender/Element} child
         */
        add: function (child) {
            if (child && child !== this && child.parent !== this) {

                this._children.push(child);

                this._doAdd(child);
            }

            return this;
        },

        /**
         * 添加子节点在 nextSibling 之前
         * @param {module:zrender/Element} child
         * @param {module:zrender/Element} nextSibling
         */
        addBefore: function (child, nextSibling) {
            if (child && child !== this && child.parent !== this
                && nextSibling && nextSibling.parent === this) {

                var children = this._children;
                var idx = children.indexOf(nextSibling);

                if (idx >= 0) {
                    children.splice(idx, 0, child);
                    this._doAdd(child);
                }
            }

            return this;
        },

        _doAdd: function (child) {
            if (child.parent) {
                child.parent.remove(child);
            }

            child.parent = this;

            var storage = this.__storage;
            var zr = this.__zr;
            if (storage && storage !== child.__storage) {

                storage.addToStorage(child);

                if (child instanceof Group) {
                    child.addChildrenToStorage(storage);
                }
            }

            zr && zr.refresh();
        },

        /**
         * 移除子节点
         * @param {module:zrender/Element} child
         */
        remove: function (child) {
            var zr = this.__zr;
            var storage = this.__storage;
            var children = this._children;

            var idx = zrUtil.indexOf(children, child);
            if (idx < 0) {
                return this;
            }
            children.splice(idx, 1);

            child.parent = null;

            if (storage) {

                storage.delFromStorage(child);

                if (child instanceof Group) {
                    child.delChildrenFromStorage(storage);
                }
            }

            zr && zr.refresh();

            return this;
        },

        /**
         * 移除所有子节点
         */
        removeAll: function () {
            var children = this._children;
            var storage = this.__storage;
            var child;
            var i;
            for (i = 0; i < children.length; i++) {
                child = children[i];
                if (storage) {
                    storage.delFromStorage(child);
                    if (child instanceof Group) {
                        child.delChildrenFromStorage(storage);
                    }
                }
                child.parent = null;
            }
            children.length = 0;

            return this;
        },

        /**
         * 遍历所有子节点
         * @param  {Function} cb
         * @param  {}   context
         */
        eachChild: function (cb, context) {
            var children = this._children;
            for (var i = 0; i < children.length; i++) {
                var child = children[i];
                cb.call(context, child, i);
            }
            return this;
        },

        /**
         * 深度优先遍历所有子孙节点
         * @param  {Function} cb
         * @param  {}   context
         */
        traverse: function (cb, context) {
            for (var i = 0; i < this._children.length; i++) {
                var child = this._children[i];
                cb.call(context, child);

                if (child.type === 'group') {
                    child.traverse(cb, context);
                }
            }
            return this;
        },

        addChildrenToStorage: function (storage) {
            for (var i = 0; i < this._children.length; i++) {
                var child = this._children[i];
                storage.addToStorage(child);
                if (child instanceof Group) {
                    child.addChildrenToStorage(storage);
                }
            }
        },

        delChildrenFromStorage: function (storage) {
            for (var i = 0; i < this._children.length; i++) {
                var child = this._children[i];
                storage.delFromStorage(child);
                if (child instanceof Group) {
                    child.delChildrenFromStorage(storage);
                }
            }
        },

        dirty: function () {
            this.__dirty = true;
            this.__zr && this.__zr.refresh();
            return this;
        },

        /**
         * @return {module:zrender/core/BoundingRect}
         */
        getBoundingRect: function (includeChildren) {
            // TODO Caching
            var rect = null;
            var tmpRect = new BoundingRect(0, 0, 0, 0);
            var children = includeChildren || this._children;
            var tmpMat = [];

            for (var i = 0; i < children.length; i++) {
                var child = children[i];
                if (child.ignore || child.invisible) {
                    continue;
                }

                var childRect = child.getBoundingRect();
                var transform = child.getLocalTransform(tmpMat);
                // TODO
                // The boundingRect cacluated by transforming original
                // rect may be bigger than the actual bundingRect when rotation
                // is used. (Consider a circle rotated aginst its center, where
                // the actual boundingRect should be the same as that not be
                // rotated.) But we can not find better approach to calculate
                // actual boundingRect yet, considering performance.
                if (transform) {
                    tmpRect.copy(childRect);
                    tmpRect.applyTransform(transform);
                    rect = rect || tmpRect.clone();
                    rect.union(tmpRect);
                }
                else {
                    rect = rect || childRect.clone();
                    rect.union(childRect);
                }
            }
            return rect || tmpRect;
        }
    };

    zrUtil.inherits(Group, Element);

    module.exports = Group;


/***/ }),
/* 54 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Image element
 * @module zrender/graphic/Image
 */



    var Displayable = __webpack_require__(11);
    var BoundingRect = __webpack_require__(3);
    var zrUtil = __webpack_require__(0);

    var LRU = __webpack_require__(17);
    var globalImageCache = new LRU(50);
    /**
     * @alias zrender/graphic/Image
     * @extends module:zrender/graphic/Displayable
     * @constructor
     * @param {Object} opts
     */
    function ZImage(opts) {
        Displayable.call(this, opts);
    }

    ZImage.prototype = {

        constructor: ZImage,

        type: 'image',

        brush: function (ctx, prevEl) {
            var style = this.style;
            var src = style.image;
            var image;

            // Must bind each time
            style.bind(ctx, this, prevEl);
            // style.image is a url string
            if (typeof src === 'string') {
                image = this._image;
            }
            // style.image is an HTMLImageElement or HTMLCanvasElement or Canvas
            else {
                image = src;
            }
            // FIXME Case create many images with src
            if (!image && src) {
                // Try get from global image cache
                var cachedImgObj = globalImageCache.get(src);
                if (!cachedImgObj) {
                    // Create a new image
                    image = new Image();
                    image.onload = function () {
                        image.onload = null;
                        for (var i = 0; i < cachedImgObj.pending.length; i++) {
                            cachedImgObj.pending[i].dirty();
                        }
                    };
                    cachedImgObj = {
                        image: image,
                        pending: [this]
                    };
                    image.src = src;
                    globalImageCache.put(src, cachedImgObj);
                    this._image = image;
                    return;
                }
                else {
                    image = cachedImgObj.image;
                    this._image = image;
                    // Image is not complete finish, add to pending list
                    if (!image.width || !image.height) {
                        cachedImgObj.pending.push(this);
                        return;
                    }
                }
            }

            if (image) {
                // 图片已经加载完成
                // if (image.nodeName.toUpperCase() == 'IMG') {
                //     if (!image.complete) {
                //         return;
                //     }
                // }
                // Else is canvas

                var x = style.x || 0;
                var y = style.y || 0;
                // 图片加载失败
                if (!image.width || !image.height) {
                    return;
                }
                var width = style.width;
                var height = style.height;
                var aspect = image.width / image.height;
                if (width == null && height != null) {
                    // Keep image/height ratio
                    width = height * aspect;
                }
                else if (height == null && width != null) {
                    height = width / aspect;
                }
                else if (width == null && height == null) {
                    width = image.width;
                    height = image.height;
                }

                // 设置transform
                this.setTransform(ctx);

                if (style.sWidth && style.sHeight) {
                    var sx = style.sx || 0;
                    var sy = style.sy || 0;
                    ctx.drawImage(
                        image,
                        sx, sy, style.sWidth, style.sHeight,
                        x, y, width, height
                    );
                }
                else if (style.sx && style.sy) {
                    var sx = style.sx;
                    var sy = style.sy;
                    var sWidth = width - sx;
                    var sHeight = height - sy;
                    ctx.drawImage(
                        image,
                        sx, sy, sWidth, sHeight,
                        x, y, width, height
                    );
                }
                else {
                    ctx.drawImage(image, x, y, width, height);
                }

                this.restoreTransform(ctx);

                // Draw rect text
                if (style.text != null) {
                    this.drawRectText(ctx, this.getBoundingRect());
                }

            }
        },

        getBoundingRect: function () {
            var style = this.style;
            if (! this._rect) {
                this._rect = new BoundingRect(
                    style.x || 0, style.y || 0, style.width || 0, style.height || 0
                );
            }
            return this._rect;
        }
    };

    zrUtil.inherits(ZImage, Displayable);

    module.exports = ZImage;


/***/ }),
/* 55 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Text element
 * @module zrender/graphic/Text
 *
 * TODO Wrapping
 *
 * Text not support gradient
 */



    var Displayable = __webpack_require__(11);
    var zrUtil = __webpack_require__(0);
    var textContain = __webpack_require__(5);

    /**
     * @alias zrender/graphic/Text
     * @extends module:zrender/graphic/Displayable
     * @constructor
     * @param {Object} opts
     */
    var Text = function (opts) {
        Displayable.call(this, opts);
    };

    Text.prototype = {

        constructor: Text,

        type: 'text',

        brush: function (ctx, prevEl) {
            var style = this.style;
            var x = style.x || 0;
            var y = style.y || 0;
            // Convert to string
            var text = style.text;

            // Convert to string
            text != null && (text += '');

            // Always bind style
            style.bind(ctx, this, prevEl);

            if (text) {

                this.setTransform(ctx);

                var textBaseline;
                var textAlign = style.textAlign;
                var font = style.textFont || style.font;
                if (style.textVerticalAlign) {
                    var rect = textContain.getBoundingRect(
                        text, font, style.textAlign, 'top'
                    );
                    // Ignore textBaseline
                    textBaseline = 'middle';
                    switch (style.textVerticalAlign) {
                        case 'middle':
                            y -= rect.height / 2 - rect.lineHeight / 2;
                            break;
                        case 'bottom':
                            y -= rect.height - rect.lineHeight / 2;
                            break;
                        default:
                            y += rect.lineHeight / 2;
                    }
                }
                else {
                    textBaseline = style.textBaseline;
                }

                // TODO Invalid font
                ctx.font = font || '12px sans-serif';
                ctx.textAlign = textAlign || 'left';
                // Use canvas default left textAlign. Giving invalid value will cause state not change
                if (ctx.textAlign !== textAlign) {
                    ctx.textAlign = 'left';
                }
                // FIXME in text contain default is top
                ctx.textBaseline = textBaseline || 'alphabetic';
                // Use canvas default alphabetic baseline
                if (ctx.textBaseline !== textBaseline) {
                    ctx.textBaseline = 'alphabetic';
                }

                var lineHeight = textContain.measureText('国', ctx.font).width;

                var textLines = text.split('\n');
                for (var i = 0; i < textLines.length; i++) {
                    // Fill after stroke so the outline will not cover the main part.
                    style.hasStroke() && ctx.strokeText(textLines[i], x, y);
                    style.hasFill() && ctx.fillText(textLines[i], x, y);
                    y += lineHeight;
                }

                this.restoreTransform(ctx);
            }
        },

        getBoundingRect: function () {
            var style = this.style;
            if (!this._rect) {
                var textVerticalAlign = style.textVerticalAlign;
                var rect = textContain.getBoundingRect(
                    style.text + '', style.textFont || style.font, style.textAlign,
                    textVerticalAlign ? 'top' : style.textBaseline
                );
                switch (textVerticalAlign) {
                    case 'middle':
                        rect.y -= rect.height / 2;
                        break;
                    case 'bottom':
                        rect.y -= rect.height;
                        break;
                }
                rect.x += style.x || 0;
                rect.y += style.y || 0;
                if (style.hasStroke()) {
                    var w = style.lineWidth;
                    rect.x -= w / 2;
                    rect.y -= w / 2;
                    rect.width += w;
                    rect.height += w;
                }
                this._rect = rect;
            }

            return this._rect;
        }
    };

    zrUtil.inherits(Text, Displayable);

    module.exports = Text;


/***/ }),
/* 56 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * 圆形
 * @module zrender/shape/Circle
 */



    module.exports = __webpack_require__(1).extend({

        type: 'circle',

        shape: {
            cx: 0,
            cy: 0,
            r: 0
        },


        buildPath : function (ctx, shape, inBundle) {
            // Better stroking in ShapeBundle
            // Always do it may have performence issue ( fill may be 2x more cost)
            if (inBundle) {
                ctx.moveTo(shape.cx + shape.r, shape.cy);
            }
            // else {
            //     if (ctx.allocate && !ctx.data.length) {
            //         ctx.allocate(ctx.CMD_MEM_SIZE.A);
            //     }
            // }
            // Better stroking in ShapeBundle
            // ctx.moveTo(shape.cx + shape.r, shape.cy);
            ctx.arc(shape.cx, shape.cy, shape.r, 0, Math.PI * 2, true);
        }
    });



/***/ }),
/* 57 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 扇形
 * @module zrender/graphic/shape/Sector
 */



    var env = __webpack_require__(13);
    var Path = __webpack_require__(1);

    var shadowTemp = [
        ['shadowBlur', 0],
        ['shadowColor', '#000'],
        ['shadowOffsetX', 0],
        ['shadowOffsetY', 0]
    ];

    module.exports = Path.extend({

        type: 'sector',

        shape: {

            cx: 0,

            cy: 0,

            r0: 0,

            r: 0,

            startAngle: 0,

            endAngle: Math.PI * 2,

            clockwise: true
        },

        brush: (env.browser.ie && env.browser.version >= 11) // version: '11.0'
            // Fix weird bug in some version of IE11 (like 11.0.9600.17801),
            // where exception "unexpected call to method or property access"
            // might be thrown when calling ctx.fill after a path whose area size
            // is zero is drawn and ctx.clip() is called and shadowBlur is set.
            // (e.g.,
            //  ctx.moveTo(10, 10);
            //  ctx.lineTo(20, 10);
            //  ctx.closePath();
            //  ctx.clip();
            //  ctx.shadowBlur = 10;
            //  ...
            //  ctx.fill();
            // )
            ? function () {
                var clipPaths = this.__clipPaths;
                var style = this.style;
                var modified;

                if (clipPaths) {
                    for (var i = 0; i < clipPaths.length; i++) {
                        var shape = clipPaths[i] && clipPaths[i].shape;
                        if (shape && shape.startAngle === shape.endAngle) {
                            for (var j = 0; j < shadowTemp.length; j++) {
                                shadowTemp[j][2] = style[shadowTemp[j][0]];
                                style[shadowTemp[j][0]] = shadowTemp[j][1];
                            }
                            modified = true;
                            break;
                        }
                    }
                }

                Path.prototype.brush.apply(this, arguments);

                if (modified) {
                    for (var j = 0; j < shadowTemp.length; j++) {
                        style[shadowTemp[j][0]] = shadowTemp[j][2];
                    }
                }
            }
            : Path.prototype.brush,

        buildPath: function (ctx, shape) {

            var x = shape.cx;
            var y = shape.cy;
            var r0 = Math.max(shape.r0 || 0, 0);
            var r = Math.max(shape.r, 0);
            var startAngle = shape.startAngle;
            var endAngle = shape.endAngle;
            var clockwise = shape.clockwise;

            var unitX = Math.cos(startAngle);
            var unitY = Math.sin(startAngle);

            ctx.moveTo(unitX * r0 + x, unitY * r0 + y);

            ctx.lineTo(unitX * r + x, unitY * r + y);

            ctx.arc(x, y, r, startAngle, endAngle, !clockwise);

            ctx.lineTo(
                Math.cos(endAngle) * r0 + x,
                Math.sin(endAngle) * r0 + y
            );

            if (r0 !== 0) {
                ctx.arc(x, y, r0, endAngle, startAngle, clockwise);
            }

            ctx.closePath();
        }
    });



/***/ }),
/* 58 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 圆环
 * @module zrender/graphic/shape/Ring
 */


    module.exports = __webpack_require__(1).extend({

        type: 'ring',

        shape: {
            cx: 0,
            cy: 0,
            r: 0,
            r0: 0
        },

        buildPath: function (ctx, shape) {
            var x = shape.cx;
            var y = shape.cy;
            var PI2 = Math.PI * 2;
            ctx.moveTo(x + shape.r, y);
            ctx.arc(x, y, shape.r, 0, PI2, false);
            ctx.moveTo(x + shape.r0, y);
            ctx.arc(x, y, shape.r0, 0, PI2, true);
        }
    });



/***/ }),
/* 59 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 多边形
 * @module zrender/shape/Polygon
 */


    var polyHelper = __webpack_require__(20);

    module.exports = __webpack_require__(1).extend({
        
        type: 'polygon',

        shape: {
            points: null,

            smooth: false,

            smoothConstraint: null
        },

        buildPath: function (ctx, shape) {
            polyHelper.buildPath(ctx, shape, true);
        }
    });


/***/ }),
/* 60 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * Catmull-Rom spline 插值折线
 * @module zrender/shape/util/smoothSpline
 * @author pissang (https://www.github.com/pissang)
 *         Kener (@Kener-林峰, kener.linfeng@gmail.com)
 *         errorrik (errorrik@gmail.com)
 */

    var vec2 = __webpack_require__(2);

    /**
     * @inner
     */
    function interpolate(p0, p1, p2, p3, t, t2, t3) {
        var v0 = (p2 - p0) * 0.5;
        var v1 = (p3 - p1) * 0.5;
        return (2 * (p1 - p2) + v0 + v1) * t3
                + (-3 * (p1 - p2) - 2 * v0 - v1) * t2
                + v0 * t + p1;
    }

    /**
     * @alias module:zrender/shape/util/smoothSpline
     * @param {Array} points 线段顶点数组
     * @param {boolean} isLoop
     * @return {Array}
     */
    module.exports = function (points, isLoop) {
        var len = points.length;
        var ret = [];

        var distance = 0;
        for (var i = 1; i < len; i++) {
            distance += vec2.distance(points[i - 1], points[i]);
        }

        var segs = distance / 2;
        segs = segs < len ? len : segs;
        for (var i = 0; i < segs; i++) {
            var pos = i / (segs - 1) * (isLoop ? len : len - 1);
            var idx = Math.floor(pos);

            var w = pos - idx;

            var p0;
            var p1 = points[idx % len];
            var p2;
            var p3;
            if (!isLoop) {
                p0 = points[idx === 0 ? idx : idx - 1];
                p2 = points[idx > len - 2 ? len - 1 : idx + 1];
                p3 = points[idx > len - 3 ? len - 1 : idx + 2];
            }
            else {
                p0 = points[(idx - 1 + len) % len];
                p2 = points[(idx + 1) % len];
                p3 = points[(idx + 2) % len];
            }

            var w2 = w * w;
            var w3 = w * w2;

            ret.push([
                interpolate(p0[0], p1[0], p2[0], p3[0], w, w2, w3),
                interpolate(p0[1], p1[1], p2[1], p3[1], w, w2, w3)
            ]);
        }
        return ret;
    };



/***/ }),
/* 61 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 贝塞尔平滑曲线
 * @module zrender/shape/util/smoothBezier
 * @author pissang (https://www.github.com/pissang)
 *         Kener (@Kener-林峰, kener.linfeng@gmail.com)
 *         errorrik (errorrik@gmail.com)
 */


    var vec2 = __webpack_require__(2);
    var v2Min = vec2.min;
    var v2Max = vec2.max;
    var v2Scale = vec2.scale;
    var v2Distance = vec2.distance;
    var v2Add = vec2.add;

    /**
     * 贝塞尔平滑曲线
     * @alias module:zrender/shape/util/smoothBezier
     * @param {Array} points 线段顶点数组
     * @param {number} smooth 平滑等级, 0-1
     * @param {boolean} isLoop
     * @param {Array} constraint 将计算出来的控制点约束在一个包围盒内
     *                           比如 [[0, 0], [100, 100]], 这个包围盒会与
     *                           整个折线的包围盒做一个并集用来约束控制点。
     * @param {Array} 计算出来的控制点数组
     */
    module.exports = function (points, smooth, isLoop, constraint) {
        var cps = [];

        var v = [];
        var v1 = [];
        var v2 = [];
        var prevPoint;
        var nextPoint;

        var min, max;
        if (constraint) {
            min = [Infinity, Infinity];
            max = [-Infinity, -Infinity];
            for (var i = 0, len = points.length; i < len; i++) {
                v2Min(min, min, points[i]);
                v2Max(max, max, points[i]);
            }
            // 与指定的包围盒做并集
            v2Min(min, min, constraint[0]);
            v2Max(max, max, constraint[1]);
        }

        for (var i = 0, len = points.length; i < len; i++) {
            var point = points[i];

            if (isLoop) {
                prevPoint = points[i ? i - 1 : len - 1];
                nextPoint = points[(i + 1) % len];
            }
            else {
                if (i === 0 || i === len - 1) {
                    cps.push(vec2.clone(points[i]));
                    continue;
                }
                else {
                    prevPoint = points[i - 1];
                    nextPoint = points[i + 1];
                }
            }

            vec2.sub(v, nextPoint, prevPoint);

            // use degree to scale the handle length
            v2Scale(v, v, smooth);

            var d0 = v2Distance(point, prevPoint);
            var d1 = v2Distance(point, nextPoint);
            var sum = d0 + d1;
            if (sum !== 0) {
                d0 /= sum;
                d1 /= sum;
            }

            v2Scale(v1, v, -d0);
            v2Scale(v2, v, d1);
            var cp0 = v2Add([], point, v1);
            var cp1 = v2Add([], point, v2);
            if (constraint) {
                v2Max(cp0, cp0, min);
                v2Min(cp0, cp0, max);
                v2Max(cp1, cp1, min);
                v2Min(cp1, cp1, max);
            }
            cps.push(cp0);
            cps.push(cp1);
        }

        if (isLoop) {
            cps.push(cps.shift());
        }

        return cps;
    };



/***/ }),
/* 62 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * @module zrender/graphic/shape/Polyline
 */


    var polyHelper = __webpack_require__(20);

    module.exports = __webpack_require__(1).extend({
        
        type: 'polyline',

        shape: {
            points: null,

            smooth: false,

            smoothConstraint: null
        },

        style: {
            stroke: '#000',

            fill: null
        },

        buildPath: function (ctx, shape) {
            polyHelper.buildPath(ctx, shape, false);
        }
    });


/***/ }),
/* 63 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 矩形
 * @module zrender/graphic/shape/Rect
 */


    var roundRectHelper = __webpack_require__(64);

    module.exports = __webpack_require__(1).extend({

        type: 'rect',

        shape: {
            // 左上、右上、右下、左下角的半径依次为r1、r2、r3、r4
            // r缩写为1         相当于 [1, 1, 1, 1]
            // r缩写为[1]       相当于 [1, 1, 1, 1]
            // r缩写为[1, 2]    相当于 [1, 2, 1, 2]
            // r缩写为[1, 2, 3] 相当于 [1, 2, 3, 2]
            r: 0,

            x: 0,
            y: 0,
            width: 0,
            height: 0
        },

        buildPath: function (ctx, shape) {
            var x = shape.x;
            var y = shape.y;
            var width = shape.width;
            var height = shape.height;
            if (!shape.r) {
                ctx.rect(x, y, width, height);
            }
            else {
                roundRectHelper.buildPath(ctx, shape);
            }
            ctx.closePath();
            return;
        }
    });



/***/ }),
/* 64 */
/***/ (function(module, exports) {



    module.exports = {
        buildPath: function (ctx, shape) {
            var x = shape.x;
            var y = shape.y;
            var width = shape.width;
            var height = shape.height;
            var r = shape.r;
            var r1;
            var r2;
            var r3;
            var r4;

            // Convert width and height to positive for better borderRadius
            if (width < 0) {
                x = x + width;
                width = -width;
            }
            if (height < 0) {
                y = y + height;
                height = -height;
            }

            if (typeof r === 'number') {
                r1 = r2 = r3 = r4 = r;
            }
            else if (r instanceof Array) {
                if (r.length === 1) {
                    r1 = r2 = r3 = r4 = r[0];
                }
                else if (r.length === 2) {
                    r1 = r3 = r[0];
                    r2 = r4 = r[1];
                }
                else if (r.length === 3) {
                    r1 = r[0];
                    r2 = r4 = r[1];
                    r3 = r[2];
                }
                else {
                    r1 = r[0];
                    r2 = r[1];
                    r3 = r[2];
                    r4 = r[3];
                }
            }
            else {
                r1 = r2 = r3 = r4 = 0;
            }

            var total;
            if (r1 + r2 > width) {
                total = r1 + r2;
                r1 *= width / total;
                r2 *= width / total;
            }
            if (r3 + r4 > width) {
                total = r3 + r4;
                r3 *= width / total;
                r4 *= width / total;
            }
            if (r2 + r3 > height) {
                total = r2 + r3;
                r2 *= height / total;
                r3 *= height / total;
            }
            if (r1 + r4 > height) {
                total = r1 + r4;
                r1 *= height / total;
                r4 *= height / total;
            }
            ctx.moveTo(x + r1, y);
            ctx.lineTo(x + width - r2, y);
            r2 !== 0 && ctx.quadraticCurveTo(
                x + width, y, x + width, y + r2
            );
            ctx.lineTo(x + width, y + height - r3);
            r3 !== 0 && ctx.quadraticCurveTo(
                x + width, y + height, x + width - r3, y + height
            );
            ctx.lineTo(x + r4, y + height);
            r4 !== 0 && ctx.quadraticCurveTo(
                x, y + height, x, y + height - r4
            );
            ctx.lineTo(x, y + r1);
            r1 !== 0 && ctx.quadraticCurveTo(x, y, x + r1, y);
        }
    };


/***/ }),
/* 65 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 直线
 * @module zrender/graphic/shape/Line
 */

    module.exports = __webpack_require__(1).extend({

        type: 'line',

        shape: {
            // Start point
            x1: 0,
            y1: 0,
            // End point
            x2: 0,
            y2: 0,

            percent: 1
        },

        style: {
            stroke: '#000',
            fill: null
        },

        buildPath: function (ctx, shape) {
            var x1 = shape.x1;
            var y1 = shape.y1;
            var x2 = shape.x2;
            var y2 = shape.y2;
            var percent = shape.percent;

            if (percent === 0) {
                return;
            }

            ctx.moveTo(x1, y1);

            if (percent < 1) {
                x2 = x1 * (1 - percent) + x2 * percent;
                y2 = y1 * (1 - percent) + y2 * percent;
            }
            ctx.lineTo(x2, y2);
        },

        /**
         * Get point at percent
         * @param  {number} percent
         * @return {Array.<number>}
         */
        pointAt: function (p) {
            var shape = this.shape;
            return [
                shape.x1 * (1 - p) + shape.x2 * p,
                shape.y1 * (1 - p) + shape.y2 * p
            ];
        }
    });



/***/ }),
/* 66 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

/**
 * 贝塞尔曲线
 * @module zrender/shape/BezierCurve
 */


    var curveTool = __webpack_require__(4);
    var vec2 = __webpack_require__(2);
    var quadraticSubdivide = curveTool.quadraticSubdivide;
    var cubicSubdivide = curveTool.cubicSubdivide;
    var quadraticAt = curveTool.quadraticAt;
    var cubicAt = curveTool.cubicAt;
    var quadraticDerivativeAt = curveTool.quadraticDerivativeAt;
    var cubicDerivativeAt = curveTool.cubicDerivativeAt;

    var out = [];

    function someVectorAt(shape, t, isTangent) {
        var cpx2 = shape.cpx2;
        var cpy2 = shape.cpy2;
        if (cpx2 === null || cpy2 === null) {
            return [
                (isTangent ? cubicDerivativeAt : cubicAt)(shape.x1, shape.cpx1, shape.cpx2, shape.x2, t),
                (isTangent ? cubicDerivativeAt : cubicAt)(shape.y1, shape.cpy1, shape.cpy2, shape.y2, t)
            ];
        }
        else {
            return [
                (isTangent ? quadraticDerivativeAt : quadraticAt)(shape.x1, shape.cpx1, shape.x2, t),
                (isTangent ? quadraticDerivativeAt : quadraticAt)(shape.y1, shape.cpy1, shape.y2, t)
            ];
        }
    }
    module.exports = __webpack_require__(1).extend({

        type: 'bezier-curve',

        shape: {
            x1: 0,
            y1: 0,
            x2: 0,
            y2: 0,
            cpx1: 0,
            cpy1: 0,
            // cpx2: 0,
            // cpy2: 0

            // Curve show percent, for animating
            percent: 1
        },

        style: {
            stroke: '#000',
            fill: null
        },

        buildPath: function (ctx, shape) {
            var x1 = shape.x1;
            var y1 = shape.y1;
            var x2 = shape.x2;
            var y2 = shape.y2;
            var cpx1 = shape.cpx1;
            var cpy1 = shape.cpy1;
            var cpx2 = shape.cpx2;
            var cpy2 = shape.cpy2;
            var percent = shape.percent;
            if (percent === 0) {
                return;
            }

            ctx.moveTo(x1, y1);

            if (cpx2 == null || cpy2 == null) {
                if (percent < 1) {
                    quadraticSubdivide(
                        x1, cpx1, x2, percent, out
                    );
                    cpx1 = out[1];
                    x2 = out[2];
                    quadraticSubdivide(
                        y1, cpy1, y2, percent, out
                    );
                    cpy1 = out[1];
                    y2 = out[2];
                }

                ctx.quadraticCurveTo(
                    cpx1, cpy1,
                    x2, y2
                );
            }
            else {
                if (percent < 1) {
                    cubicSubdivide(
                        x1, cpx1, cpx2, x2, percent, out
                    );
                    cpx1 = out[1];
                    cpx2 = out[2];
                    x2 = out[3];
                    cubicSubdivide(
                        y1, cpy1, cpy2, y2, percent, out
                    );
                    cpy1 = out[1];
                    cpy2 = out[2];
                    y2 = out[3];
                }
                ctx.bezierCurveTo(
                    cpx1, cpy1,
                    cpx2, cpy2,
                    x2, y2
                );
            }
        },

        /**
         * Get point at percent
         * @param  {number} t
         * @return {Array.<number>}
         */
        pointAt: function (t) {
            return someVectorAt(this.shape, t, false);
        },

        /**
         * Get tangent at percent
         * @param  {number} t
         * @return {Array.<number>}
         */
        tangentAt: function (t) {
            var p = someVectorAt(this.shape, t, true);
            return vec2.normalize(p, p);
        }
    });



/***/ }),
/* 67 */
/***/ (function(module, exports, __webpack_require__) {

/**
 * 圆弧
 * @module zrender/graphic/shape/Arc
 */
 

    module.exports = __webpack_require__(1).extend({

        type: 'arc',

        shape: {

            cx: 0,

            cy: 0,

            r: 0,

            startAngle: 0,

            endAngle: Math.PI * 2,

            clockwise: true
        },

        style: {

            stroke: '#000',

            fill: null
        },

        buildPath: function (ctx, shape) {

            var x = shape.cx;
            var y = shape.cy;
            var r = Math.max(shape.r, 0);
            var startAngle = shape.startAngle;
            var endAngle = shape.endAngle;
            var clockwise = shape.clockwise;

            var unitX = Math.cos(startAngle);
            var unitY = Math.sin(startAngle);

            ctx.moveTo(unitX * r + x, unitY * r + y);
            ctx.arc(x, y, r, startAngle, endAngle, !clockwise);
        }
    });


/***/ }),
/* 68 */
/***/ (function(module, exports, __webpack_require__) {

// CompoundPath to improve performance


    var Path = __webpack_require__(1);

    module.exports = Path.extend({

        type: 'compound',

        shape: {

            paths: null
        },

        _updatePathDirty: function () {
            var dirtyPath = this.__dirtyPath;
            var paths = this.shape.paths;
            for (var i = 0; i < paths.length; i++) {
                // Mark as dirty if any subpath is dirty
                dirtyPath = dirtyPath || paths[i].__dirtyPath;
            }
            this.__dirtyPath = dirtyPath;
            this.__dirty = this.__dirty || dirtyPath;
        },

        beforeBrush: function () {
            this._updatePathDirty();
            var paths = this.shape.paths || [];
            var scale = this.getGlobalScale();
            // Update path scale
            for (var i = 0; i < paths.length; i++) {
                if (!paths[i].path) {
                    paths[i].createPathProxy();
                }
                paths[i].path.setScale(scale[0], scale[1]);
            }
        },

        buildPath: function (ctx, shape) {
            var paths = shape.paths || [];
            for (var i = 0; i < paths.length; i++) {
                paths[i].buildPath(ctx, paths[i].shape, true);
            }
        },

        afterBrush: function () {
            var paths = this.shape.paths;
            for (var i = 0; i < paths.length; i++) {
                paths[i].__dirtyPath = false;
            }
        },

        getBoundingRect: function () {
            this._updatePathDirty();
            return Path.prototype.getBoundingRect.call(this);
        }
    });


/***/ }),
/* 69 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";



    var zrUtil = __webpack_require__(0);

    var Gradient = __webpack_require__(21);

    /**
     * x, y, x2, y2 are all percent from 0 to 1
     * @param {number} [x=0]
     * @param {number} [y=0]
     * @param {number} [x2=1]
     * @param {number} [y2=0]
     * @param {Array.<Object>} colorStops
     * @param {boolean} [globalCoord=false]
     */
    var LinearGradient = function (x, y, x2, y2, colorStops, globalCoord) {
        // Should do nothing more in this constructor. Because gradient can be
        // declard by `color: {type: 'linear', colorStops: ...}`, where
        // this constructor will not be called.

        this.x = x == null ? 0 : x;

        this.y = y == null ? 0 : y;

        this.x2 = x2 == null ? 1 : x2;

        this.y2 = y2 == null ? 0 : y2;

        // Can be cloned
        this.type = 'linear';

        // If use global coord
        this.global = globalCoord || false;

        Gradient.call(this, colorStops);
    };

    LinearGradient.prototype = {

        constructor: LinearGradient
    };

    zrUtil.inherits(LinearGradient, Gradient);

    module.exports = LinearGradient;


/***/ }),
/* 70 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";



    var zrUtil = __webpack_require__(0);

    var Gradient = __webpack_require__(21);

    /**
     * x, y, r are all percent from 0 to 1
     * @param {number} [x=0.5]
     * @param {number} [y=0.5]
     * @param {number} [r=0.5]
     * @param {Array.<Object>} [colorStops]
     * @param {boolean} [globalCoord=false]
     */
    var RadialGradient = function (x, y, r, colorStops, globalCoord) {
        // Should do nothing more in this constructor. Because gradient can be
        // declard by `color: {type: 'radial', colorStops: ...}`, where
        // this constructor will not be called.

        this.x = x == null ? 0.5 : x;

        this.y = y == null ? 0.5 : y;

        this.r = r == null ? 0.5 : r;

        // Can be cloned
        this.type = 'radial';

        // If use global coord
        this.global = globalCoord || false;

        Gradient.call(this, colorStops);
    };

    RadialGradient.prototype = {

        constructor: RadialGradient
    };

    zrUtil.inherits(RadialGradient, Gradient);

    module.exports = RadialGradient;


/***/ }),
/* 71 */
/***/ (function(module, exports, __webpack_require__) {


    var getItemStyle = __webpack_require__(10)(
        [
            ['fill', 'color'],
            ['stroke', 'borderColor'],
            ['lineWidth', 'borderWidth'],
            ['opacity'],
            ['shadowBlur'],
            ['shadowOffsetX'],
            ['shadowOffsetY'],
            ['shadowColor'],
            ['textPosition'],
            ['textAlign']
        ]
    );
    module.exports = {
        getItemStyle: function (excludes, includes) {
            var style = getItemStyle.call(this, excludes, includes);
            var lineDash = this.getBorderLineDash();
            lineDash && (style.lineDash = lineDash);
            return style;
        },

        getBorderLineDash: function () {
            var lineType = this.get('borderType');
            return (lineType === 'solid' || lineType == null) ? null
                : (lineType === 'dashed' ? [5, 5] : [1, 1]);
        }
    };


/***/ }),
/* 72 */
/***/ (function(module, exports, __webpack_require__) {

var echarts = __webpack_require__(7);

function getShallow(model, path) {
    return model && model.getShallow(path);
}

echarts.extendChartView({

    type: 'wordCloud',

    render: function (seriesModel, ecModel, api) {
        var group = this.group;
        group.removeAll();

        var data = seriesModel.getData();

        var gridSize = seriesModel.get('gridSize');

        seriesModel.layoutInstance.ondraw = function (text, size, dataIdx, drawn) {
            var itemModel = data.getItemModel(dataIdx);
            var textStyleModel = itemModel.getModel('textStyle.normal');
            var emphasisTextStyleModel = itemModel.getModel('textStyle.emphasis');

            var getFont = function (model, otherModel) {
                var ecModel = model.ecModel;
                var gTextStyleModel = ecModel && ecModel.getModel('textStyle');
                return ['fontStyle', 'fontWeight', 'fontSize', 'fontFamily'].map(function (name, idx) {
                    if (idx !== 2) {
                        return model.getShallow(name)
                                || otherModel.getShallow(name)
                                || getShallow(gTextStyleModel, name);
                    }
                    else {
                        return (
                            model.getShallow(name, true)
                            || Math.round(
                                    model === textStyleModel
                                    ? size : (otherModel.getShallow(name, true) || size)
                                )
                        ) + 'px';
                    }
                }).join(' ');
            };
            var text = new echarts.graphic.Text({
                style: {
                    x: drawn.info.fillTextOffsetX,
                    y: drawn.info.fillTextOffsetY + size * 0.5,
                    text: text,
                    textBaseline: 'middle',
                    font: getFont(textStyleModel, emphasisTextStyleModel)
                },
                scale: [1 / drawn.info.mu, 1 / drawn.info.mu],
                position: [
                    (drawn.gx + drawn.info.gw / 2) * gridSize,
                    (drawn.gy + drawn.info.gh / 2) * gridSize
                ],
                rotation: drawn.rot
            });

            text.setStyle(textStyleModel.getItemStyle());

            text.setStyle({
                fill: data.getItemVisual(dataIdx, 'color')
            });

            group.add(text);

            data.setItemGraphicEl(dataIdx, text);
            echarts.graphic.setHoverStyle(
                text, echarts.util.extend(
                    emphasisTextStyleModel.getItemStyle(),
                    {
                        font: getFont(emphasisTextStyleModel, textStyleModel)
                    }
                )
            );
        };

        this._model = seriesModel;
    },

    remove: function () {
        this.group.removeAll();

        this._model.layoutInstance.dispose();
    },

    dispose: function () {
        this._model.layoutInstance.dispose();
    }
});

/***/ }),
/* 73 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
var __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;/*!
 * wordcloud2.js
 * http://timdream.org/wordcloud2.js/
 *
 * Copyright 2011 - 2013 Tim Chien
 * Released under the MIT license
 */



// setImmediate
if (!window.setImmediate) {
  window.setImmediate = (function setupSetImmediate() {
    return window.msSetImmediate ||
    window.webkitSetImmediate ||
    window.mozSetImmediate ||
    window.oSetImmediate ||
    (function setupSetZeroTimeout() {
      if (!window.postMessage || !window.addEventListener) {
        return null;
      }

      var callbacks = [undefined];
      var message = 'zero-timeout-message';

      // Like setTimeout, but only takes a function argument.  There's
      // no time argument (always zero) and no arguments (you have to
      // use a closure).
      var setZeroTimeout = function setZeroTimeout(callback) {
        var id = callbacks.length;
        callbacks.push(callback);
        window.postMessage(message + id.toString(36), '*');

        return id;
      };

      window.addEventListener('message', function setZeroTimeoutMessage(evt) {
        // Skipping checking event source, retarded IE confused this window
        // object with another in the presence of iframe
        if (typeof evt.data !== 'string' ||
            evt.data.substr(0, message.length) !== message/* ||
            evt.source !== window */) {
          return;
        }

        evt.stopImmediatePropagation();

        var id = parseInt(evt.data.substr(message.length), 36);
        if (!callbacks[id]) {
          return;
        }

        callbacks[id]();
        callbacks[id] = undefined;
      }, true);

      /* specify clearImmediate() here since we need the scope */
      window.clearImmediate = function clearZeroTimeout(id) {
        if (!callbacks[id]) {
          return;
        }

        callbacks[id] = undefined;
      };

      return setZeroTimeout;
    })() ||
    // fallback
    function setImmediateFallback(fn) {
      window.setTimeout(fn, 0);
    };
  })();
}

if (!window.clearImmediate) {
  window.clearImmediate = (function setupClearImmediate() {
    return window.msClearImmediate ||
    window.webkitClearImmediate ||
    window.mozClearImmediate ||
    window.oClearImmediate ||
    // "clearZeroTimeout" is implement on the previous block ||
    // fallback
    function clearImmediateFallback(timer) {
      window.clearTimeout(timer);
    };
  })();
}

(function(global) {

  // Check if WordCloud can run on this browser
  var isSupported = (function isSupported() {
    var canvas = document.createElement('canvas');
    if (!canvas || !canvas.getContext) {
      return false;
    }

    var ctx = canvas.getContext('2d');
    if (!ctx.getImageData) {
      return false;
    }
    if (!ctx.fillText) {
      return false;
    }

    if (!Array.prototype.some) {
      return false;
    }
    if (!Array.prototype.push) {
      return false;
    }

    return true;
  }());

  // Find out if the browser impose minium font size by
  // drawing small texts on a canvas and measure it's width.
  var minFontSize = (function getMinFontSize() {
    if (!isSupported) {
      return;
    }

    var ctx = document.createElement('canvas').getContext('2d');

    // start from 20
    var size = 20;

    // two sizes to measure
    var hanWidth, mWidth;

    while (size) {
      ctx.font = size.toString(10) + 'px sans-serif';
      if ((ctx.measureText('\uFF37').width === hanWidth) &&
          (ctx.measureText('m').width) === mWidth) {
        return (size + 1);
      }

      hanWidth = ctx.measureText('\uFF37').width;
      mWidth = ctx.measureText('m').width;

      size--;
    }

    return 0;
  })();

  // Based on http://jsfromhell.com/array/shuffle
  var shuffleArray = function shuffleArray(arr) {
    for (var j, x, i = arr.length; i;
      j = Math.floor(Math.random() * i),
      x = arr[--i], arr[i] = arr[j],
      arr[j] = x) {}
    return arr;
  };

  var WordCloud = function WordCloud(elements, options) {
    if (!isSupported) {
      return;
    }

    if (!Array.isArray(elements)) {
      elements = [elements];
    }

    elements.forEach(function(el, i) {
      if (typeof el === 'string') {
        elements[i] = document.getElementById(el);
        if (!elements[i]) {
          throw 'The element id specified is not found.';
        }
      } else if (!el.tagName && !el.appendChild) {
        throw 'You must pass valid HTML elements, or ID of the element.';
      }
    });

    /* Default values to be overwritten by options object */
    var settings = {
      list: [],
      fontFamily: '"Trebuchet MS", "Heiti TC", "微軟正黑體", ' +
                  '"Arial Unicode MS", "Droid Fallback Sans", sans-serif',
      fontWeight: 'normal',
      color: 'random-dark',
      minSize: 0, // 0 to disable
      weightFactor: 1,
      clearCanvas: true,
      backgroundColor: '#fff',  // opaque white = rgba(255, 255, 255, 1)

      gridSize: 8,
      drawOutOfBound: false,
      origin: null,

      drawMask: false,
      maskColor: 'rgba(255,0,0,0.3)',
      maskGapWidth: 0.3,

      wait: 0,
      abortThreshold: 0, // disabled
      abort: function noop() {},

      minRotation: - Math.PI / 2,
      maxRotation: Math.PI / 2,
      rotationStep: 0.1,

      shuffle: true,
      rotateRatio: 0.1,

      shape: 'circle',
      ellipticity: 0.65,

      classes: null,

      hover: null,
      click: null
    };

    if (options) {
      for (var key in options) {
        if (key in settings) {
          settings[key] = options[key];
        }
      }
    }

    /* Convert weightFactor into a function */
    if (typeof settings.weightFactor !== 'function') {
      var factor = settings.weightFactor;
      settings.weightFactor = function weightFactor(pt) {
        return pt * factor; //in px
      };
    }

    /* Convert shape into a function */
    if (typeof settings.shape !== 'function') {
      switch (settings.shape) {
        case 'circle':
        /* falls through */
        default:
          // 'circle' is the default and a shortcut in the code loop.
          settings.shape = 'circle';
          break;

        case 'cardioid':
          settings.shape = function shapeCardioid(theta) {
            return 1 - Math.sin(theta);
          };
          break;

        /*
        To work out an X-gon, one has to calculate "m",
        where 1/(cos(2*PI/X)+m*sin(2*PI/X)) = 1/(cos(0)+m*sin(0))
        http://www.wolframalpha.com/input/?i=1%2F%28cos%282*PI%2FX%29%2Bm*sin%28
        2*PI%2FX%29%29+%3D+1%2F%28cos%280%29%2Bm*sin%280%29%29
        Copy the solution into polar equation r = 1/(cos(t') + m*sin(t'))
        where t' equals to mod(t, 2PI/X);
        */

        case 'diamond':
        case 'square':
          // http://www.wolframalpha.com/input/?i=plot+r+%3D+1%2F%28cos%28mod+
          // %28t%2C+PI%2F2%29%29%2Bsin%28mod+%28t%2C+PI%2F2%29%29%29%2C+t+%3D
          // +0+..+2*PI
          settings.shape = function shapeSquare(theta) {
            var thetaPrime = theta % (2 * Math.PI / 4);
            return 1 / (Math.cos(thetaPrime) + Math.sin(thetaPrime));
          };
          break;

        case 'triangle-forward':
          // http://www.wolframalpha.com/input/?i=plot+r+%3D+1%2F%28cos%28mod+
          // %28t%2C+2*PI%2F3%29%29%2Bsqrt%283%29sin%28mod+%28t%2C+2*PI%2F3%29
          // %29%29%2C+t+%3D+0+..+2*PI
          settings.shape = function shapeTriangle(theta) {
            var thetaPrime = theta % (2 * Math.PI / 3);
            return 1 / (Math.cos(thetaPrime) +
                        Math.sqrt(3) * Math.sin(thetaPrime));
          };
          break;

        case 'triangle':
        case 'triangle-upright':
          settings.shape = function shapeTriangle(theta) {
            var thetaPrime = (theta + Math.PI * 3 / 2) % (2 * Math.PI / 3);
            return 1 / (Math.cos(thetaPrime) +
                        Math.sqrt(3) * Math.sin(thetaPrime));
          };
          break;

        case 'pentagon':
          settings.shape = function shapePentagon(theta) {
            var thetaPrime = (theta + 0.955) % (2 * Math.PI / 5);
            return 1 / (Math.cos(thetaPrime) +
                        0.726543 * Math.sin(thetaPrime));
          };
          break;

        case 'star':
          settings.shape = function shapeStar(theta) {
            var thetaPrime = (theta + 0.955) % (2 * Math.PI / 10);
            if ((theta + 0.955) % (2 * Math.PI / 5) - (2 * Math.PI / 10) >= 0) {
              return 1 / (Math.cos((2 * Math.PI / 10) - thetaPrime) +
                          3.07768 * Math.sin((2 * Math.PI / 10) - thetaPrime));
            } else {
              return 1 / (Math.cos(thetaPrime) +
                          3.07768 * Math.sin(thetaPrime));
            }
          };
          break;
      }
    }

    /* Make sure gridSize is a whole number and is not smaller than 4px */
    settings.gridSize = Math.max(Math.floor(settings.gridSize), 4);

    /* shorthand */
    var g = settings.gridSize;
    var maskRectWidth = g - settings.maskGapWidth;

    /* normalize rotation settings */
    var rotationRange = Math.abs(settings.maxRotation - settings.minRotation);
    var minRotation = Math.min(settings.maxRotation, settings.minRotation);
    var rotationStep = settings.rotationStep;

    /* information/object available to all functions, set when start() */
    var grid, // 2d array containing filling information
      ngx, ngy, // width and height of the grid
      center, // position of the center of the cloud
      maxRadius;

    /* timestamp for measuring each putWord() action */
    var escapeTime;

    /* function for getting the color of the text */
    var getTextColor;
    function random_hsl_color(min, max) {
      return 'hsl(' +
        (Math.random() * 360).toFixed() + ',' +
        (Math.random() * 30 + 70).toFixed() + '%,' +
        (Math.random() * (max - min) + min).toFixed() + '%)';
    }
    switch (settings.color) {
      case 'random-dark':
        getTextColor = function getRandomDarkColor() {
          return random_hsl_color(10, 50);
        };
        break;

      case 'random-light':
        getTextColor = function getRandomLightColor() {
          return random_hsl_color(50, 90);
        };
        break;

      default:
        if (typeof settings.color === 'function') {
          getTextColor = settings.color;
        }
        break;
    }

    /* function for getting the classes of the text */
    var getTextClasses = null;
    if (typeof settings.classes === 'function') {
      getTextClasses = settings.classes;
    }

    /* Interactive */
    var interactive = false;
    var infoGrid = [];
    var hovered;

    var getInfoGridFromMouseTouchEvent =
    function getInfoGridFromMouseTouchEvent(evt) {
      var canvas = evt.currentTarget;
      var rect = canvas.getBoundingClientRect();
      var clientX;
      var clientY;
      /** Detect if touches are available */
      if (evt.touches) {
        clientX = evt.touches[0].clientX;
        clientY = evt.touches[0].clientY;
      } else {
        clientX = evt.clientX;
        clientY = evt.clientY;
      }
      var eventX = clientX - rect.left;
      var eventY = clientY - rect.top;

      var x = Math.floor(eventX * ((canvas.width / rect.width) || 1) / g);
      var y = Math.floor(eventY * ((canvas.height / rect.height) || 1) / g);

      return infoGrid[x][y];
    };

    var wordcloudhover = function wordcloudhover(evt) {
      var info = getInfoGridFromMouseTouchEvent(evt);

      if (hovered === info) {
        return;
      }

      hovered = info;
      if (!info) {
        settings.hover(undefined, undefined, evt);

        return;
      }

      settings.hover(info.item, info.dimension, evt);

    };

    var wordcloudclick = function wordcloudclick(evt) {
      var info = getInfoGridFromMouseTouchEvent(evt);
      if (!info) {
        return;
      }

      settings.click(info.item, info.dimension, evt);
      evt.preventDefault();
    };

    /* Get points on the grid for a given radius away from the center */
    var pointsAtRadius = [];
    var getPointsAtRadius = function getPointsAtRadius(radius) {
      if (pointsAtRadius[radius]) {
        return pointsAtRadius[radius];
      }

      // Look for these number of points on each radius
      var T = radius * 8;

      // Getting all the points at this radius
      var t = T;
      var points = [];

      if (radius === 0) {
        points.push([center[0], center[1], 0]);
      }

      while (t--) {
        // distort the radius to put the cloud in shape
        var rx = 1;
        if (settings.shape !== 'circle') {
          rx = settings.shape(t / T * 2 * Math.PI); // 0 to 1
        }

        // Push [x, y, t]; t is used solely for getTextColor()
        points.push([
          center[0] + radius * rx * Math.cos(-t / T * 2 * Math.PI),
          center[1] + radius * rx * Math.sin(-t / T * 2 * Math.PI) *
            settings.ellipticity,
          t / T * 2 * Math.PI]);
      }

      pointsAtRadius[radius] = points;
      return points;
    };

    /* Return true if we had spent too much time */
    var exceedTime = function exceedTime() {
      return ((settings.abortThreshold > 0) &&
        ((new Date()).getTime() - escapeTime > settings.abortThreshold));
    };

    /* Get the deg of rotation according to settings, and luck. */
    var getRotateDeg = function getRotateDeg() {
      if (settings.rotateRatio === 0) {
        return 0;
      }

      if (Math.random() > settings.rotateRatio) {
        return 0;
      }

      if (rotationRange === 0) {
        return minRotation;
      }

      return minRotation + Math.round(Math.random() * rotationRange / rotationStep) * rotationStep;
    };

    var getTextInfo = function getTextInfo(word, weight, rotateDeg) {
      // calculate the acutal font size
      // fontSize === 0 means weightFactor function wants the text skipped,
      // and size < minSize means we cannot draw the text.
      var debug = false;
      var fontSize = settings.weightFactor(weight);
      if (fontSize <= settings.minSize) {
        return false;
      }

      // Scale factor here is to make sure fillText is not limited by
      // the minium font size set by browser.
      // It will always be 1 or 2n.
      var mu = 1;
      if (fontSize < minFontSize) {
        mu = (function calculateScaleFactor() {
          var mu = 2;
          while (mu * fontSize < minFontSize) {
            mu += 2;
          }
          return mu;
        })();
      }

      var fcanvas = document.createElement('canvas');
      var fctx = fcanvas.getContext('2d', { willReadFrequently: true });

      fctx.font = settings.fontWeight + ' ' +
        (fontSize * mu).toString(10) + 'px ' + settings.fontFamily;

      // Estimate the dimension of the text with measureText().
      var fw = fctx.measureText(word).width / mu;
      var fh = Math.max(fontSize * mu,
                        fctx.measureText('m').width,
                        fctx.measureText('\uFF37').width) / mu;

      // Create a boundary box that is larger than our estimates,
      // so text don't get cut of (it sill might)
      var boxWidth = fw + fh * 2;
      var boxHeight = fh * 3;
      var fgw = Math.ceil(boxWidth / g);
      var fgh = Math.ceil(boxHeight / g);
      boxWidth = fgw * g;
      boxHeight = fgh * g;

      // Calculate the proper offsets to make the text centered at
      // the preferred position.

      // This is simply half of the width.
      var fillTextOffsetX = - fw / 2;
      // Instead of moving the box to the exact middle of the preferred
      // position, for Y-offset we move 0.4 instead, so Latin alphabets look
      // vertical centered.
      var fillTextOffsetY = - fh * 0.4;

      // Calculate the actual dimension of the canvas, considering the rotation.
      var cgh = Math.ceil((boxWidth * Math.abs(Math.sin(rotateDeg)) +
                           boxHeight * Math.abs(Math.cos(rotateDeg))) / g);
      var cgw = Math.ceil((boxWidth * Math.abs(Math.cos(rotateDeg)) +
                           boxHeight * Math.abs(Math.sin(rotateDeg))) / g);
      var width = cgw * g;
      var height = cgh * g;

      fcanvas.setAttribute('width', width);
      fcanvas.setAttribute('height', height);

      if (debug) {
        // Attach fcanvas to the DOM
        document.body.appendChild(fcanvas);
        // Save it's state so that we could restore and draw the grid correctly.
        fctx.save();
      }

      // Scale the canvas with |mu|.
      fctx.scale(1 / mu, 1 / mu);
      fctx.translate(width * mu / 2, height * mu / 2);
      fctx.rotate(- rotateDeg);

      // Once the width/height is set, ctx info will be reset.
      // Set it again here.
      fctx.font = settings.fontWeight + ' ' +
        (fontSize * mu).toString(10) + 'px ' + settings.fontFamily;

      // Fill the text into the fcanvas.
      // XXX: We cannot because textBaseline = 'top' here because
      // Firefox and Chrome uses different default line-height for canvas.
      // Please read https://bugzil.la/737852#c6.
      // Here, we use textBaseline = 'middle' and draw the text at exactly
      // 0.5 * fontSize lower.
      fctx.fillStyle = '#000';
      fctx.textBaseline = 'middle';
      fctx.fillText(word, fillTextOffsetX * mu,
                    (fillTextOffsetY + fontSize * 0.5) * mu);

      // Get the pixels of the text
      var imageData = fctx.getImageData(0, 0, width, height).data;

      if (exceedTime()) {
        return false;
      }

      if (debug) {
        // Draw the box of the original estimation
        fctx.strokeRect(fillTextOffsetX * mu,
                        fillTextOffsetY, fw * mu, fh * mu);
        fctx.restore();
      }

      // Read the pixels and save the information to the occupied array
      var occupied = [];
      var gx = cgw, gy, x, y;
      var bounds = [cgh / 2, cgw / 2, cgh / 2, cgw / 2];
      while (gx--) {
        gy = cgh;
        while (gy--) {
          y = g;
          singleGridLoop: {
            while (y--) {
              x = g;
              while (x--) {
                if (imageData[((gy * g + y) * width +
                               (gx * g + x)) * 4 + 3]) {
                  occupied.push([gx, gy]);

                  if (gx < bounds[3]) {
                    bounds[3] = gx;
                  }
                  if (gx > bounds[1]) {
                    bounds[1] = gx;
                  }
                  if (gy < bounds[0]) {
                    bounds[0] = gy;
                  }
                  if (gy > bounds[2]) {
                    bounds[2] = gy;
                  }

                  if (debug) {
                    fctx.fillStyle = 'rgba(255, 0, 0, 0.5)';
                    fctx.fillRect(gx * g, gy * g, g - 0.5, g - 0.5);
                  }
                  break singleGridLoop;
                }
              }
            }
            if (debug) {
              fctx.fillStyle = 'rgba(0, 0, 255, 0.5)';
              fctx.fillRect(gx * g, gy * g, g - 0.5, g - 0.5);
            }
          }
        }
      }

      if (debug) {
        fctx.fillStyle = 'rgba(0, 255, 0, 0.5)';
        fctx.fillRect(bounds[3] * g,
                      bounds[0] * g,
                      (bounds[1] - bounds[3] + 1) * g,
                      (bounds[2] - bounds[0] + 1) * g);
      }

      // Return information needed to create the text on the real canvas
      return {
        mu: mu,
        occupied: occupied,
        bounds: bounds,
        gw: cgw,
        gh: cgh,
        fillTextOffsetX: fillTextOffsetX,
        fillTextOffsetY: fillTextOffsetY,
        fillTextWidth: fw,
        fillTextHeight: fh,
        fontSize: fontSize
      };
    };

    /* Determine if there is room available in the given dimension */
    var canFitText = function canFitText(gx, gy, gw, gh, occupied) {
      // Go through the occupied points,
      // return false if the space is not available.
      var i = occupied.length;
      while (i--) {
        var px = gx + occupied[i][0];
        var py = gy + occupied[i][1];

        if (px >= ngx || py >= ngy || px < 0 || py < 0) {
          if (!settings.drawOutOfBound) {
            return false;
          }
          continue;
        }

        if (!grid[px][py]) {
          return false;
        }
      }
      return true;
    };

    /* Actually draw the text on the grid */
    var drawText = function drawText(gx, gy, info, word, weight,
                                     distance, theta, rotateDeg, attributes) {

      var fontSize = info.fontSize;
      var color;
      if (getTextColor) {
        color = getTextColor(word, weight, fontSize, distance, theta);
      } else {
        color = settings.color;
      }

      var classes;
      if (getTextClasses) {
        classes = getTextClasses(word, weight, fontSize, distance, theta);
      } else {
        classes = settings.classes;
      }

      var dimension;
      var bounds = info.bounds;
      dimension = {
        x: (gx + bounds[3]) * g,
        y: (gy + bounds[0]) * g,
        w: (bounds[1] - bounds[3] + 1) * g,
        h: (bounds[2] - bounds[0] + 1) * g
      };

      elements.forEach(function(el) {
        if (el.getContext) {
          var ctx = el.getContext('2d');
          var mu = info.mu;

          // Save the current state before messing it
          ctx.save();
          ctx.scale(1 / mu, 1 / mu);

          ctx.font = settings.fontWeight + ' ' +
                     (fontSize * mu).toString(10) + 'px ' + settings.fontFamily;
          ctx.fillStyle = color;

          // Translate the canvas position to the origin coordinate of where
          // the text should be put.
          ctx.translate((gx + info.gw / 2) * g * mu,
                        (gy + info.gh / 2) * g * mu);

          if (rotateDeg !== 0) {
            ctx.rotate(- rotateDeg);
          }

          // Finally, fill the text.

          // XXX: We cannot because textBaseline = 'top' here because
          // Firefox and Chrome uses different default line-height for canvas.
          // Please read https://bugzil.la/737852#c6.
          // Here, we use textBaseline = 'middle' and draw the text at exactly
          // 0.5 * fontSize lower.
          ctx.textBaseline = 'middle';
          ctx.fillText(word, info.fillTextOffsetX * mu,
                             (info.fillTextOffsetY + fontSize * 0.5) * mu);

          // The below box is always matches how <span>s are positioned
          /* ctx.strokeRect(info.fillTextOffsetX, info.fillTextOffsetY,
            info.fillTextWidth, info.fillTextHeight); */

          // Restore the state.
          ctx.restore();
        } else {
          // drawText on DIV element
          var span = document.createElement('span');
          var transformRule = '';
          transformRule = 'rotate(' + (- rotateDeg / Math.PI * 180) + 'deg) ';
          if (info.mu !== 1) {
            transformRule +=
              'translateX(-' + (info.fillTextWidth / 4) + 'px) ' +
              'scale(' + (1 / info.mu) + ')';
          }
          var styleRules = {
            'position': 'absolute',
            'display': 'block',
            'font': settings.fontWeight + ' ' +
                    (fontSize * info.mu) + 'px ' + settings.fontFamily,
            'left': ((gx + info.gw / 2) * g + info.fillTextOffsetX) + 'px',
            'top': ((gy + info.gh / 2) * g + info.fillTextOffsetY) + 'px',
            'width': info.fillTextWidth + 'px',
            'height': info.fillTextHeight + 'px',
            'lineHeight': fontSize + 'px',
            'whiteSpace': 'nowrap',
            'transform': transformRule,
            'webkitTransform': transformRule,
            'msTransform': transformRule,
            'transformOrigin': '50% 40%',
            'webkitTransformOrigin': '50% 40%',
            'msTransformOrigin': '50% 40%'
          };
          if (color) {
            styleRules.color = color;
          }
          span.textContent = word;
          for (var cssProp in styleRules) {
            span.style[cssProp] = styleRules[cssProp];
          }
          if (attributes) {
            for (var attribute in attributes) {
              span.setAttribute(attribute, attributes[attribute]);
            }
          }
          if (classes) {
            span.className += classes;
          }
          el.appendChild(span);
        }
      });
    };

    /* Help function to updateGrid */
    var fillGridAt = function fillGridAt(x, y, drawMask, dimension, item) {
      if (x >= ngx || y >= ngy || x < 0 || y < 0) {
        return;
      }

      grid[x][y] = false;

      if (drawMask) {
        var ctx = elements[0].getContext('2d');
        ctx.fillRect(x * g, y * g, maskRectWidth, maskRectWidth);
      }

      if (interactive) {
        infoGrid[x][y] = { item: item, dimension: dimension };
      }
    };

    /* Update the filling information of the given space with occupied points.
       Draw the mask on the canvas if necessary. */
    var updateGrid = function updateGrid(gx, gy, gw, gh, info, item) {
      var occupied = info.occupied;
      var drawMask = settings.drawMask;
      var ctx;
      if (drawMask) {
        ctx = elements[0].getContext('2d');
        ctx.save();
        ctx.fillStyle = settings.maskColor;
      }

      var dimension;
      if (interactive) {
        var bounds = info.bounds;
        dimension = {
          x: (gx + bounds[3]) * g,
          y: (gy + bounds[0]) * g,
          w: (bounds[1] - bounds[3] + 1) * g,
          h: (bounds[2] - bounds[0] + 1) * g
        };
      }

      var i = occupied.length;
      while (i--) {
        var px = gx + occupied[i][0];
        var py = gy + occupied[i][1];

        if (px >= ngx || py >= ngy || px < 0 || py < 0) {
          continue;
        }

        fillGridAt(px, py, drawMask, dimension, item);
      }

      if (drawMask) {
        ctx.restore();
      }
    };

    /* putWord() processes each item on the list,
       calculate it's size and determine it's position, and actually
       put it on the canvas. */
    var putWord = function putWord(item) {
      var word, weight, attributes;
      if (Array.isArray(item)) {
        word = item[0];
        weight = item[1];
      } else {
        word = item.word;
        weight = item.weight;
        attributes = item.attributes;
      }
      var rotateDeg = getRotateDeg();

      // get info needed to put the text onto the canvas
      var info = getTextInfo(word, weight, rotateDeg);

      // not getting the info means we shouldn't be drawing this one.
      if (!info) {
        return false;
      }

      if (exceedTime()) {
        return false;
      }

      // If drawOutOfBound is set to false,
      // skip the loop if we have already know the bounding box of
      // word is larger than the canvas.
      if (!settings.drawOutOfBound) {
        var bounds = info.bounds;
        if ((bounds[1] - bounds[3] + 1) > ngx ||
          (bounds[2] - bounds[0] + 1) > ngy) {
          return false;
        }
      }

      // Determine the position to put the text by
      // start looking for the nearest points
      var r = maxRadius + 1;

      var tryToPutWordAtPoint = function(gxy) {
        var gx = Math.floor(gxy[0] - info.gw / 2);
        var gy = Math.floor(gxy[1] - info.gh / 2);
        var gw = info.gw;
        var gh = info.gh;

        // If we cannot fit the text at this position, return false
        // and go to the next position.
        if (!canFitText(gx, gy, gw, gh, info.occupied)) {
          return false;
        }

        // Actually put the text on the canvas
        drawText(gx, gy, info, word, weight,
                 (maxRadius - r), gxy[2], rotateDeg, attributes);

        // Mark the spaces on the grid as filled
        updateGrid(gx, gy, gw, gh, info, item);

        return {
          gx: gx,
          gy: gy,
          rot: rotateDeg,
          info: info
        };
      };

      while (r--) {
        var points = getPointsAtRadius(maxRadius - r);

        if (settings.shuffle) {
          points = [].concat(points);
          shuffleArray(points);
        }

        // Try to fit the words by looking at each point.
        // array.some() will stop and return true
        // when putWordAtPoint() returns true.
        for (var i = 0; i < points.length; i++) {
          var res = tryToPutWordAtPoint(points[i]);
          if (res) {
            return res;
          }
        }

        // var drawn = points.some(tryToPutWordAtPoint);
        // if (drawn) {
        //   // leave putWord() and return true
        //   return true;
        // }
      }
      // we tried all distances but text won't fit, return null
      return null;
    };

    /* Send DOM event to all elements. Will stop sending event and return
       if the previous one is canceled (for cancelable events). */
    var sendEvent = function sendEvent(type, cancelable, detail) {
      if (cancelable) {
        return !elements.some(function(el) {
          var evt = document.createEvent('CustomEvent');
          evt.initCustomEvent(type, true, cancelable, detail || {});
          return !el.dispatchEvent(evt);
        }, this);
      } else {
        elements.forEach(function(el) {
          var evt = document.createEvent('CustomEvent');
          evt.initCustomEvent(type, true, cancelable, detail || {});
          el.dispatchEvent(evt);
        }, this);
      }
    };

    /* Start drawing on a canvas */
    var start = function start() {
      // For dimensions, clearCanvas etc.,
      // we only care about the first element.
      var canvas = elements[0];

      if (canvas.getContext) {
        ngx = Math.ceil(canvas.width / g);
        ngy = Math.ceil(canvas.height / g);
      } else {
        var rect = canvas.getBoundingClientRect();
        ngx = Math.ceil(rect.width / g);
        ngy = Math.ceil(rect.height / g);
      }

      // Sending a wordcloudstart event which cause the previous loop to stop.
      // Do nothing if the event is canceled.
      if (!sendEvent('wordcloudstart', true)) {
        return;
      }

      // Determine the center of the word cloud
      center = (settings.origin) ?
        [settings.origin[0]/g, settings.origin[1]/g] :
        [ngx / 2, ngy / 2];

      // Maxium radius to look for space
      maxRadius = Math.floor(Math.sqrt(ngx * ngx + ngy * ngy));

      /* Clear the canvas only if the clearCanvas is set,
         if not, update the grid to the current canvas state */
      grid = [];

      var gx, gy, i;
      if (!canvas.getContext || settings.clearCanvas) {
        elements.forEach(function(el) {
          if (el.getContext) {
            var ctx = el.getContext('2d');
            ctx.fillStyle = settings.backgroundColor;
            ctx.clearRect(0, 0, ngx * (g + 1), ngy * (g + 1));
            ctx.fillRect(0, 0, ngx * (g + 1), ngy * (g + 1));
          } else {
            el.textContent = '';
            el.style.backgroundColor = settings.backgroundColor;
            el.style.position = 'relative';
          }
        });

        /* fill the grid with empty state */
        gx = ngx;
        while (gx--) {
          grid[gx] = [];
          gy = ngy;
          while (gy--) {
            grid[gx][gy] = true;
          }
        }
      } else {
        /* Determine bgPixel by creating
           another canvas and fill the specified background color. */
        var bctx = document.createElement('canvas').getContext('2d');

        bctx.fillStyle = settings.backgroundColor;
        bctx.fillRect(0, 0, 1, 1);
        var bgPixel = bctx.getImageData(0, 0, 1, 1).data;

        /* Read back the pixels of the canvas we got to tell which part of the
           canvas is empty.
           (no clearCanvas only works with a canvas, not divs) */
        var imageData =
          canvas.getContext('2d').getImageData(0, 0, ngx * g, ngy * g).data;

        gx = ngx;
        var x, y;
        while (gx--) {
          grid[gx] = [];
          gy = ngy;
          while (gy--) {
            y = g;
            singleGridLoop: while (y--) {
              x = g;
              while (x--) {
                i = 4;
                while (i--) {
                  if (imageData[((gy * g + y) * ngx * g +
                                 (gx * g + x)) * 4 + i] !== bgPixel[i]) {
                    grid[gx][gy] = false;
                    break singleGridLoop;
                  }
                }
              }
            }
            if (grid[gx][gy] !== false) {
              grid[gx][gy] = true;
            }
          }
        }

        imageData = bctx = bgPixel = undefined;
      }

      // fill the infoGrid with empty state if we need it
      if (settings.hover || settings.click) {

        interactive = true;

        /* fill the grid with empty state */
        gx = ngx + 1;
        while (gx--) {
          infoGrid[gx] = [];
        }

        if (settings.hover) {
          canvas.addEventListener('mousemove', wordcloudhover);
        }

        if (settings.click) {
          canvas.addEventListener('click', wordcloudclick);
          canvas.addEventListener('touchstart', wordcloudclick);
          canvas.addEventListener('touchend', function (e) {
            e.preventDefault();
          });
          canvas.style.webkitTapHighlightColor = 'rgba(0, 0, 0, 0)';
        }

        canvas.addEventListener('wordcloudstart', function stopInteraction() {
          canvas.removeEventListener('wordcloudstart', stopInteraction);

          canvas.removeEventListener('mousemove', wordcloudhover);
          canvas.removeEventListener('click', wordcloudclick);
          hovered = undefined;
        });
      }

      i = 0;
      var loopingFunction, stoppingFunction;
      if (settings.wait !== 0) {
        loopingFunction = window.setTimeout;
        stoppingFunction = window.clearTimeout;
      } else {
        loopingFunction = window.setImmediate;
        stoppingFunction = window.clearImmediate;
      }

      var addEventListener = function addEventListener(type, listener) {
        elements.forEach(function(el) {
          el.addEventListener(type, listener);
        }, this);
      };

      var removeEventListener = function removeEventListener(type, listener) {
        elements.forEach(function(el) {
          el.removeEventListener(type, listener);
        }, this);
      };

      var anotherWordCloudStart = function anotherWordCloudStart() {
        removeEventListener('wordcloudstart', anotherWordCloudStart);
        stoppingFunction(timer);
      };

      addEventListener('wordcloudstart', anotherWordCloudStart);

      var timer = loopingFunction(function loop() {
        if (i >= settings.list.length) {
          stoppingFunction(timer);
          sendEvent('wordcloudstop', false);
          removeEventListener('wordcloudstart', anotherWordCloudStart);

          return;
        }
        escapeTime = (new Date()).getTime();
        var drawn = putWord(settings.list[i]);
        var canceled = !sendEvent('wordclouddrawn', true, {
          item: settings.list[i], drawn: drawn });
        if (exceedTime() || canceled) {
          stoppingFunction(timer);
          settings.abort();
          sendEvent('wordcloudabort', false);
          sendEvent('wordcloudstop', false);
          removeEventListener('wordcloudstart', anotherWordCloudStart);
          return;
        }
        i++;
        timer = loopingFunction(loop, settings.wait);
      }, settings.wait);
    };

    // All set, start the drawing
    start();
  };

  WordCloud.isSupported = isSupported;
  WordCloud.minFontSize = minFontSize;

  // Expose the library as an AMD module
  if (true) {
    !(__WEBPACK_AMD_DEFINE_ARRAY__ = [], __WEBPACK_AMD_DEFINE_RESULT__ = function() { return WordCloud; }.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__),
				__WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__));
  } else if (typeof module !== 'undefined' && module.exports) {
    module.exports = WordCloud;
  } else {
    global.WordCloud = WordCloud;
  }

})(this); //jshint ignore:line

/***/ })
/******/ ]);
});