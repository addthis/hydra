# hydra-web
This folder contains the sources for all compiled web assets.

## How to build
All commands should be run from `hydra-web/`. Remember to run `npm install`, and
install webpack globally:

```
npm install
npm install webpack -g
```

### Development
 - build the assets 1x with `webpack -d`
 - continually build on file changes with `webpack -d --watch`

### Production
 - As of Oct. 2017, you don't have to build for production.
 - This will be done when you execute deploy.py script.
 - That is, every change under hydra-web directory will be built when you run deploy script.
 - NOTE: Please install webpack on the machine in which the execution of deploy script takes place. 
  

## Where do built files go?
To keep existing deploy scripts working, all assets are built to 
`/hydra-main/web/`. 

## What is built?
Currently, only spawn2: `/hydra-main/web/spawn2/build`, but there are plans to
build `query` as well. Static assets are still in `hydra-main/web`, but should
be packaged in the webpack build moving forward.

## What language features are enabled?
 - ES6
 - ES7 decorators
 - Tree shaking