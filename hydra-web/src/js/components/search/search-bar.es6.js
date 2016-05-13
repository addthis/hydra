'use strict';

import React from 'react';
import ReactDOM from 'react-dom';
import linkState from 'react-link-state';
import {requiredPaletteProp} from 'style/color-palette';
import FloatingBottomFooter from 'components/containers/floating-bottom-footer';
import autobind from 'autobind-decorator';

export default class SearchBar extends React.Component {
    static propTypes = {
        palette: requiredPaletteProp,
        visible: React.PropTypes.bool.isRequired,
        formTarget: React.PropTypes.oneOf(['_blank', '_self', '_parent', '_top']),
        onSubmit: React.PropTypes.func
    };

    defaultProps = {
        formTarget: '_blank',
        onSubmit: null
    };

    state = {
        searchString: ''
    };

    componentWillReceiveProps(nextProps) {
        if (nextProps.visible && !this.props.visible) {
            const found = ReactDOM.findDOMNode(this.refs.searchInput);
            if (found) {
                setTimeout(() => found.select());
            }
        }
    }

    @autobind
    handleSubmit(evt) {
        // React doesn't forward the 'target' prop to the actual form element
        evt.target.target = this.props.formTarget;

        if (this.props.onSubmit) {
            this.props.onSubmit(this.state.searchString);
            evt.preventDefault();
        }
    }

    render() {
        const {
            palette,
            visible
        } = this.props;

        const {
            searchString
        } = this.state;

        const style = {
            backgroundColor: palette.background2,
            borderTop: '1px solid ' + palette.background3,
            color: palette.text0,
            position: 'fixed',
            bottom: '0px',
            height: '5em',
            width: '100%',
            fontFamily: 'sans-serif',
            fontSize: '0.8em',
            display: 'flex',
            alignItems: 'center',
            zIndex: '100'
        };

        const inputStyle = {
            backgroundColor: palette.background1,
            color: palette.text1,
            width: '97%',
            borderRadius: '5px'
        };

        const checkboxGroupStyle = {
            display: 'inline-block'
        };

        const checkboxLabelStyle = {
            display: 'block',
            padding: '5px',
            margin: '0'
        };

        const inputGroupStyle = {
            flexGrow: '1'
        };

        const action = `/spawn2/search-results.html#${searchString}`;

        return (
            <FloatingBottomFooter visible={visible}>
                <div style={style}>
                    <div style={checkboxGroupStyle}>
                        <label style={checkboxLabelStyle}>
                            <input type={'checkbox'}
                                defaultChecked={true}
                                disabled={true}
                            />
                            RegExp
                        </label>
                        <label style={checkboxLabelStyle}>
                            <input type={'checkbox'}
                                defaultChecked={true}
                                disabled={true}
                            />
                            Expand configs
                        </label>
                    </div>
                    <div style={{padding: '10px'}}>
                        Find All:
                    </div>
                    <form style={inputGroupStyle}
                        action={action}
                        method={'get'}
                        onSubmit={this.handleSubmit}>
                        <input type={'text'}
                            ref={'searchInput'}
                            style={inputStyle}
                            valueLink={linkState(this, 'searchString')}
                        />
                    </form>
                </div>
            </FloatingBottomFooter>
        );
    }
}
