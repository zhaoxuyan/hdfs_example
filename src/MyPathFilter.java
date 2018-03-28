import org.apache.hadoop.fs.*;

class MyPathFilter implements PathFilter {
    private String mReg = null;

    MyPathFilter(String reg) {
        this.mReg = reg;
    }

    @Override
    public boolean accept(Path path) {
        return !(path.toString()).matches(mReg);
    }
}