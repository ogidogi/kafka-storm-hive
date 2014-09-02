import java.util.Random;
import co.signal.loadgen.SyntheticLoadGenerator;

public class ExampleExpediaJsonGenerator implements SyntheticLoadGenerator {

        public ExampleExpediaJsonGenerator(String ignored) {}

        String[] sites = {"EXPEDIA.COM", "EXPEDIA.AU", "EXPEDIA.EU"};
        String[] local_dt = {"2014-08-01", "2014-08-02"};

        @Override
        public String nextMessage() {
                int site_idx = new Random().nextInt(sites.length);
                int ld_idx = new Random().nextInt(local_dt.length);

                String curr_site = sites[site_idx];
                String curr_dt = local_dt[ld_idx];
                long ts = System.currentTimeMillis();
                String data = ts + ": Data " + curr_site + ";" + curr_dt;

                String jsonStr = "{\"site_name\":\"" + curr_site + "\",\"local_date\":\"" + curr_dt + "\",\"data\":\"" + data + "\"}";
                //String jsonStr = curr_site + '\t' + curr_dt + '\t' +  data;

                return jsonStr;
        }

        /*
        public static void main(String[] args) {
                Dev curr_site = new Dev();

                for (int i=0; i<20; i++) {
                        System.out.println(curr_site.nextMessage());
                }
        }
        */
}
