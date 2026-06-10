/* Kafka Visuals — page-agnostic step-through engine.
 *
 * A page declares an ordered array of steps and calls:
 *   Stepper.init({ steps: steps, root: el });
 *
 * Step shape:
 *   { stages:   ["id", ...],     // [data-stage] elements to mark .is-active
 *     token:    "slotId"|null,   // [data-token-slot] where the token rests (null = hide)
 *     narration:"text",          // [data-narration] textContent
 *     code:     1|null,          // [data-code-line] value to mark .is-current (null = none)
 *     callout:  { kind:"warn"|"note", text:"..." } | undefined }
 *
 * State is fully derived from the step index — any step can be jumped to directly.
 */
(function (global) {
  'use strict';

  function toArray(nodeList) {
    return Array.prototype.slice.call(nodeList);
  }

  function Stepper(opts) {
    this.steps = (opts && opts.steps) || [];
    this.root = (opts && opts.root) || document;
    this.intervalMs = (opts && opts.intervalMs) || 2400;
    this.index = 0;
    this.timer = null;

    this.token = this.root.querySelector('[data-token]');
    this.narration = this.root.querySelector('[data-narration]');
    this.callout = this.root.querySelector('[data-callout]');
    this.counter = this.root.querySelector('[data-counter]');
    this.stages = toArray(this.root.querySelectorAll('[data-stage]'));
    this.codeLines = toArray(this.root.querySelectorAll('[data-code-line]'));

    var self = this;
    this.setEls = {};
    toArray(this.root.querySelectorAll('[data-set]')).forEach(function (el) {
      self.setEls[el.getAttribute('data-set')] = el;
    });

    this.bindControls();
    this.bindKeys();
    this.render();
  }

  Stepper.prototype.bindControls = function () {
    var self = this;
    var handlers = {
      prev: function () { self.prev(); },
      next: function () { self.next(); },
      reset: function () { self.reset(); },
      autoplay: function () { self.toggleAutoplay(); }
    };
    Object.keys(handlers).forEach(function (name) {
      var btn = self.root.querySelector('[data-control="' + name + '"]');
      if (btn) { btn.addEventListener('click', handlers[name]); }
    });
  };

  Stepper.prototype.bindKeys = function () {
    var self = this;
    document.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowRight') { self.next(); }
      else if (e.key === 'ArrowLeft') { self.prev(); }
      else if (e.key === 'Home') { self.reset(); }
      else if (e.key === ' ' || e.key === 'Spacebar') { e.preventDefault(); self.toggleAutoplay(); }
    });
  };

  Stepper.prototype.next = function () {
    if (this.index < this.steps.length - 1) { this.index += 1; this.render(); }
    else { this.stopAutoplay(); }
  };

  Stepper.prototype.prev = function () {
    this.stopAutoplay();
    if (this.index > 0) { this.index -= 1; this.render(); }
  };

  Stepper.prototype.reset = function () {
    this.stopAutoplay();
    this.index = 0;
    this.render();
  };

  Stepper.prototype.toggleAutoplay = function () {
    if (this.timer) { this.stopAutoplay(); } else { this.startAutoplay(); }
  };

  Stepper.prototype.startAutoplay = function () {
    var self = this;
    if (this.index >= this.steps.length - 1) { this.index = 0; this.render(); }
    this.timer = setInterval(function () {
      if (self.index >= self.steps.length - 1) { self.stopAutoplay(); }
      else { self.index += 1; self.render(); }
    }, this.intervalMs);
    this.setAutoplayLabel('⏸ Pause');
  };

  Stepper.prototype.stopAutoplay = function () {
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
    this.setAutoplayLabel('▶ Play');
  };

  Stepper.prototype.setAutoplayLabel = function (text) {
    var btn = this.root.querySelector('[data-control="autoplay"]');
    if (btn) { btn.textContent = text; }
  };

  Stepper.prototype.render = function () {
    var step = this.steps[this.index] || {};
    var active = step.stages || [];

    this.stages.forEach(function (el) {
      var id = el.getAttribute('data-stage');
      el.classList.toggle('is-active', active.indexOf(id) !== -1);
    });

    this.codeLines.forEach(function (el) {
      var line = parseInt(el.getAttribute('data-code-line'), 10);
      el.classList.toggle('is-current', step.code != null && line === step.code);
    });

    if (this.token) {
      if (step.token) {
        var slot = this.root.querySelector('[data-token-slot="' + step.token + '"]');
        if (slot && this.token.parentNode !== slot) { slot.appendChild(this.token); }
        this.token.classList.remove('is-hidden');
        // Re-trigger the pop animation on each move.
        this.token.style.animation = 'none';
        this.token.offsetHeight; // force reflow
        this.token.style.animation = '';
      } else {
        this.token.classList.add('is-hidden');
      }
    }

    if (this.narration) { this.narration.textContent = step.narration || ''; }

    if (this.callout) {
      if (step.callout) {
        this.callout.textContent = step.callout.text;
        this.callout.className = 'callout is-' + (step.callout.kind || 'note');
      } else {
        this.callout.className = 'callout is-hidden';
        this.callout.textContent = '';
      }
    }

    // Effective per-step text values. State is derived from the index by walking
    // steps 0..index (last value wins), so meters are correct after backward jumps.
    var effective = {};
    for (var i = 0; i <= this.index; i++) {
      var s = this.steps[i];
      if (s && s.set) {
        for (var k in s.set) {
          if (Object.prototype.hasOwnProperty.call(s.set, k)) { effective[k] = s.set[k]; }
        }
      }
    }
    for (var key in this.setEls) {
      if (Object.prototype.hasOwnProperty.call(this.setEls, key)) {
        var val = (effective[key] != null) ? effective[key] : (this.setEls[key].getAttribute('data-default') || '');
        this.setEls[key].textContent = val;
      }
    }

    if (this.counter) {
      this.counter.textContent = (this.index + 1) + ' / ' + this.steps.length;
    }

    var prevBtn = this.root.querySelector('[data-control="prev"]');
    var nextBtn = this.root.querySelector('[data-control="next"]');
    if (prevBtn) { prevBtn.disabled = this.index === 0; }
    if (nextBtn) { nextBtn.disabled = this.index === this.steps.length - 1; }
  };

  global.Stepper = {
    init: function (opts) { return new Stepper(opts); }
  };
})(window);
