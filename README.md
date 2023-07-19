# RadixSplinePlus

## Description
This project is an attempt to extend RadixSpline, a read-only learned index, so that it also supports concurrent writes. Writes are temporarily stored in a buffer that gets periodically merged with the learned index.


## Credits
The RadixSpline paper can be found [here](https://arxiv.org/abs/2004.14541)

The original RadixSpline code can be found [here](https://github.com/learnedsystems/radixspline).

For the buffer, we modified the code from the XIndex paper, which can be found [here](https://ipads.se.sjtu.edu.cn/_media/publications/tangppopp20.pdf).

The code for the Xindex can be found [here](https://ipads.se.sjtu.edu.cn:1312/opensource/xindex/-/tree/master/XIndex-R).
