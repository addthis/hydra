'use strict';

import ReactDOM from 'react-dom';

export default function (el) {
    setTimeout(() => {
        const found = ReactDOM.findDOMNode(el);

        if (found) {
            found.focus();
        }
    });
}
