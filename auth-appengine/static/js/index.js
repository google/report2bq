// import {MDCRipple} from '@material/ripple';
// import {MDCTextField} from '@material/textfield';

// import mdcAutoInit from "@material/auto-init";

// new MDCRipple(document.querySelector('.authenticate'));
mdc.ripple.attachTo(document.querySelector('.authenticate'));

document.getElementById('.authenticate').onclick = function() {
  location.href = '/authenticate';
};