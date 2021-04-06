using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Xml;
using System.Xml.Linq;
using System.IO;
using System.Web.UI;
using System.Text.RegularExpressions;

using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.Utilities;
using Thermo.Magellan.Core.Exceptions;
using Thermo.Magellan.EntityDataFramework;
using System.Threading;

namespace PD.OpenMS.AdapterNodes
{
    /// <summary>
    /// Set of delegates needed by methods in OpenMSCommons in order to be able to use the logging / messaging functionality of the nodes they're called from (HACK).
    /// </summary>
    public struct NodeDelegates
    {
        public delegate void NodeLoggerWarningDelegate(string s, params object[] args);
        public delegate Exception NodeLoggerErrorDelegate(Exception e, string s, params object[] args);
        public delegate void SendAndLogTemporaryMessageDelegate(string s, bool writeToLog = true);
        public delegate void SendAndLogMessageDelegate(string s, bool writeToLog = true);
        public delegate void SendAndLogErrorMessageDelegate(string s, bool writeToLog = true);
        public delegate void WriteLogMessageDelegate(MessageLevel ml, string s);

        public NodeLoggerWarningDelegate warnLog;
        public NodeLoggerErrorDelegate errorLog;
        public SendAndLogTemporaryMessageDelegate logTmpMessage;
        public SendAndLogMessageDelegate logMessage;
        public SendAndLogErrorMessageDelegate errorLogMessage;
        public WriteLogMessageDelegate writeLogMessage;
    }

    /// <summary>
    /// Methods that are shared between several nodes.
    /// </summary>
    public class OpenMSCommons
    {
        /// <summary>
        /// Create a default INI file for the given TOPP tool
        /// </summary>
        public static void CreateDefaultINI(string exec_path, string ini_path, string scratch_dir, NodeDelegates nd)
        {
            var timer = Stopwatch.StartNew();

            var data_path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(exec_path), @"../share/OpenMS"));
            var process_startinfo = new ProcessStartInfo();
            process_startinfo.EnvironmentVariables["OPENMS_DATA_PATH"] = data_path;
            process_startinfo.FileName = exec_path;
            process_startinfo.WorkingDirectory = scratch_dir;
            process_startinfo.Arguments = " -write_ini " + String.Format("\"{0}\"", ini_path);
            process_startinfo.UseShellExecute = false;
            process_startinfo.RedirectStandardOutput = false;
            process_startinfo.CreateNoWindow = false;

            var process = new Process
            {
                StartInfo = process_startinfo
            };

            try
            {
                process.Start();

                try
                {
                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                    throw;
                }

                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    nd.warnLog("The process [{0}] hasn't finished correctly -> force to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }
        }

        /// <summary>
        /// Write a set of parameters to the specified OpenMS INI file
        /// </summary>
        public static void WriteParamsToINI(string ini_path, Dictionary<string, string> parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                foreach (string param in parameters.Keys)
                {
                    if (item.Attributes["name"].Value == param)
                    {
                        item.Attributes["value"].Value = parameters[param];
                    }
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write a nested parameter to the specified OpenMS INI file
        /// </summary>
        public static void WriteNestedParamToINI(string ini_path, Triplet parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == parameters.First.ToString()) && (item.Attributes["name"].Value == parameters.Second.ToString()))
                {
                    item.Attributes["value"].Value = parameters.Third.ToString();
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write an item list parameter to an OpenMS INI file
        /// </summary>
        public static void WriteItemListToINI(string[] vars, string ini_path, string mode, bool clear_list_first = false)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList itemlist = doc.GetElementsByTagName("ITEMLIST");
            foreach (XmlElement item in itemlist)
            {
                //mode: in or out?
                if (item.Attributes["name"].Value == mode)
                {
                    if (clear_list_first)
                    {
                        item.IsEmpty = true;
                    }

                    foreach (var fn in vars)
                    {
                        //We add LISTITEMS to ITEMLISTS
                        var listitem = doc.CreateElement("LISTITEM");
                        XmlAttribute newAttribute = doc.CreateAttribute("value");
                        newAttribute.Value = fn;
                        listitem.SetAttributeNode(newAttribute);
                        item.AppendChild(listitem);
                    }
                }
            }
            doc.Save(ini_path);
        }

        //TODO: add version that also checks parent of parent so we don
        public static void WriteItemListToINI(string[] vars, string ini_path, string parent, string name, bool clear_list_first = false)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList itemlist = doc.GetElementsByTagName("ITEMLIST");
            foreach (XmlElement item in itemlist)
            {                
                if ((item.Attributes["name"].Value == name) // ITEMLIST name matches
                    && (item.ParentNode.Attributes["name"].Value == parent)) // parent ITEM name matches
                {
                    if (clear_list_first)
                    {
                        item.IsEmpty = true;
                    }

                    foreach (var fn in vars)
                    {
                        //We add LISTITEMS to ITEMLISTS
                        var listitem = doc.CreateElement("LISTITEM");
                        XmlAttribute newAttribute = doc.CreateAttribute("value");
                        newAttribute.Value = fn;
                        listitem.SetAttributeNode(newAttribute);
                        item.AppendChild(listitem);
                    }
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write mz and rt parameters for MapAligner or FeatureLinker. Different function than WriteParamsToINI due to specific structure in considered tools
        /// </summary>
        public static void WriteThresholdsToINI(MassToleranceParameter mz_threshold, DoubleParameter rt_threshold, string ini_path)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "max_difference"))
                {
                    var mzthresh = mz_threshold.Value.Tolerance.ToString();
                    item.Attributes["value"].Value = mzthresh;
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "unit"))
                {
                    //always use ppm
                    item.Attributes["value"].Value = "ppm";
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_RT") && (item.Attributes["name"].Value == "max_difference"))
                {
                    item.Attributes["value"].Value = (rt_threshold.Value * 60).ToString(); //need to convert minute(PD) to seconds(OpenMS)!
                }
            }
            doc.Save(ini_path);
        }




        /// <summary>
        /// Run TOPP tool. Parameters are passed via the OpenMS INI file at param_path.
        /// </summary>        
        public static void RunTOPPTool(string exec_path, string param_path, string scratch_dir, NodeDelegates nd)
        {
            // sanity check
            if (!File.Exists(exec_path))
            {
                throw new FileNotFoundException(@"[Tool not in PD folder.]");
            }

            var timer = Stopwatch.StartNew();

            var data_path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(exec_path), @"../share/OpenMS"));
            var process_startinfo = new ProcessStartInfo();
            process_startinfo.EnvironmentVariables["OPENMS_DATA_PATH"] = data_path;
            process_startinfo.FileName = exec_path;
            process_startinfo.WorkingDirectory = scratch_dir;
            process_startinfo.Arguments = " -ini " + String.Format("\"{0}\"", param_path);
            process_startinfo.UseShellExecute = false;
            process_startinfo.RedirectStandardOutput = true;
            process_startinfo.CreateNoWindow = false;
            process_startinfo.RedirectStandardError = true;

            using (Process process = new Process())
            {
                string current_work = "";
                process.StartInfo = process_startinfo;
                process.EnableRaisingEvents = true;

                StringBuilder output = new StringBuilder();
                StringBuilder error = new StringBuilder();

                using (AutoResetEvent outputWaitHandle = new AutoResetEvent(false))
                using (AutoResetEvent errorWaitHandle = new AutoResetEvent(false))
                {
                    // triggered when output data was recieved from std_out
                    process.OutputDataReceived += (sender, e) =>
                    {
                        if (e.Data == null)
                        {
                            outputWaitHandle.Set();
                        }
                        else
                        {
                            output.AppendLine(e.Data);

                            if (e.Data.Contains("%"))
                            {
                                nd.logTmpMessage(String.Format("{0} {1}", current_work, e.Data));
                            }
                            else
                            {
                                //store all results (for now?) of OpenMS Tool output
                                nd.writeLogMessage(MessageLevel.Debug, e.Data);

                                // Parse the output and report progress using the method SendAndLogTemporaryMessage
                                if (e.Data.Contains(@"Progress of 'loading mzML file':"))
                                {
                                    current_work = "Progress of 'loading mzML file':";
                                }
                                else if (e.Data.Contains("Progress of 'loading chromatograms':"))
                                {
                                    current_work = "Progress of 'loading chromatograms':";
                                }
                                else if (e.Data.Contains("Progress of 'Aligning input maps':"))
                                {
                                    current_work = "Progress of 'Aligning input maps':";
                                }
                                else if (e.Data.Contains("Progress of 'linking features':"))
                                {
                                    current_work = "Progress of 'linking features':";
                                }
                            }

                        }
                    };

                    // triggered when output data was recieved from err
                    process.ErrorDataReceived += (sender, e) =>
                    {
                        if (e.Data == null)
                        {
                            errorWaitHandle.Set();
                        }
                        else
                        {
                            error.AppendLine(e.Data);
                            //nd.writeLogMessage(MessageLevel.Error, e.Data);
                        }
                    };

                    nd.logTmpMessage(String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                            process.StartInfo.FileName,
                            process.StartInfo.WorkingDirectory,
                            process.StartInfo.Arguments));

                    nd.writeLogMessage(MessageLevel.Debug,
                                    String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                                                  process.StartInfo.FileName,
                                                  process.StartInfo.WorkingDirectory,
                                                  process.StartInfo.Arguments));
                    try
                    {
                        process.Start();
                        try
                        {
                            process.BeginOutputReadLine();
                            process.BeginErrorReadLine();

                            if (process.WaitForExit(-1) &&
                                outputWaitHandle.WaitOne(-1) &&
                                errorWaitHandle.WaitOne(-1))
                            {
                                // Process completed. Check process.ExitCode here.
                            }
                            else
                            {
                                throw new MagellanProcessingException(
                                String.Format("Process timed out {0}.",
                                              Path.GetFileName(process.StartInfo.FileName)
                                              ));
                            }
                        }
                        catch (InvalidOperationException ex)
                        {
                            nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                            throw;
                        }

                        if (process.ExitCode != 0)
                        {
                            throw new MagellanProcessingException(
                                String.Format("The exit code of {0} was {1}. (The expected exit code is 0)",
                                              Path.GetFileName(process.StartInfo.FileName),
                                              process.ExitCode));
                        }
                    }
                    catch (System.Threading.ThreadAbortException)
                    {
                        // workflow was aborted ==> kill process so PD can finish abortion
                        process.Kill();
                        throw;
                    }
                    catch (ObjectDisposedException ex) 
                    {
                        nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                        throw;
                    }
                    finally
                    {
                        if (!process.HasExited)
                        {
                            nd.warnLog("The process [{0}] hasn't finished correctly -> force to exit now", process.StartInfo.FileName);
                            // try catch might be needed because auf asynchronous execution
                            process.Kill();                                                        
                        }
                    }
                }
            }

            nd.logMessage(String.Format("{0} tool processing took {1}.", exec_path, StringHelper.GetDisplayString(timer.Elapsed)));
        }

                    /// <summary>
                    /// Return a modified sequence string (OpenMS format) from unmodified sequence + Thermo-formatted modification string
                    /// </summary>
                    public static string ModSequence(string seq, string mods_str)
                    {
                        if (mods_str == "") return seq;

                        string[] mods = Regex.Split(mods_str, "; ");
                        var actual_mods = new List<string>();

                        // WORKAROUND: PD uses "modifications" like "X1(L)" to indicate that AA X at pos. 1 is actually a leucine
                        // Substitute these before doing anything else, since "X" AAs might also be actually modified in addition
                        // to the "modifications" indicating the actual AA.
                        var tmp_seq = new StringBuilder(seq);
                        string aa_letters = "ARNDCEQGHILKMFPSTWYV";
                        foreach (string m in mods)
                        {
                            bool actual_mod = true;
                            // m is something like "M11(Oxidation)" or "N-Term(Carbamyl)" or "X8(L)"
                            string[] parts = m.Split('(');

                            if (!aa_letters.Contains(parts[0].Substring(0, 1)))
                            {
                                // modified AA character is not an actual AA (probably B, J, X, or Z)
                                // ==> now, also check if "modification" consists of just 1 letter representing an AA
                                if (parts[1].Length == 2 && aa_letters.Contains(parts[1][0]))
                                {
                                    // substitute
                                    Int32 aa_pos = Convert.ToInt32(parts[0].Substring(1));
                                    tmp_seq[aa_pos - 1] = parts[1][0];
                                    // discard this "modification"
                                    actual_mod = false;
                                }
                            }
                            if (actual_mod)
                            {
                                actual_mods.Add(m);
                            }
                        }
                        var unmodified_seq = tmp_seq.ToString();

                        var result = "";
                        var n_term_mod = "";
                        var c_term_mod = "";
                        Int32 last_pos = 0;

                        // assumption: modifications are in ascending order of AA position
                        foreach (string mm in actual_mods)
                        {
                            // remove (Prot) if present (e.g. "N-Term(Prot)(Acetyl)")
                            var m = mm.Replace("(Prot)", "");

                            // have something like "M11(Oxidation)" or "N-Term(Carbamyl)"
                            string[] parts = m.Split('(');

                            // N-term
                            if (parts[0].Length >= 6 && parts[0].Substring(0, 6) == "N-Term")
                            {
                                n_term_mod = "(" + parts[1];
                                continue;
                            }
                            // C-term
                            if (parts[0].Length >= 6 && parts[0].Substring(0, 6) == "C-Term")
                            {
                                c_term_mod = "(" + parts[1];
                                continue;
                            }
                            // Residue
                            Int32 aa_pos = Convert.ToInt32(parts[0].Substring(1));
                            Int32 substr_len = aa_pos - last_pos;
                            string mod_str = "(" + parts[1];
                            string next_chunk = unmodified_seq.Substring(last_pos, substr_len);
                            result += next_chunk + mod_str;
                            last_pos = aa_pos;
                        }
                        result = n_term_mod + result + unmodified_seq.Substring(last_pos) + c_term_mod;

                        return result;
                    }

                    /// <summary>
                    /// Remove obsolete CV terms from a set of mzML files that would otherwise lead to delay and millions of warning messages on loading in OpenMS.
                    /// </summary>
                    public static void FixCVTerms(List<string> mzml_files, NodeDelegates nd)
                    {
                        // remove in particular one obsolete CV term (MS:1000498)
                        // which otherwise leads to huge delay and millions of warning
                        // messages when loading the file into OpenMS
                        foreach (var f in mzml_files)
                        {
                            // move to temporary file
                            var tmp_f = f.Replace(".mzML", "_tmp.mzML");
                            try
                            {
                                File.Move(f, tmp_f);
                            }
                            catch (Exception)
                            {
                                nd.errorLogMessage(string.Format("Could not move file {0} to {1}", f, tmp_f));
                            }

                            // open temporary file, remove obsolete CV terms, store with original filename
                            XDocument doc = XDocument.Load(tmp_f);
                            var q = from node in doc.Descendants("{http://psi.hupo.org/ms/mzml}cvParam")
                                    let acc = node.Attribute("accession")
                                    where acc != null && acc.Value == "MS:1000498"
                                    select node;
                            q.ToList().ForEach(x => x.Remove());
                            try
                            {
                                doc.Save(f);
                            }
                            catch (Exception)
                            {
                                nd.errorLogMessage(string.Format("Could not save file {0}", f));
                            }

                            // remove temporary file
                            try
                            {
                                File.Delete(tmp_f);
                            }
                            catch (Exception)
                            {
                                nd.errorLogMessage(string.Format("Could not delete file {0}", tmp_f));
                            }
                        }
                    }

                    /// <summary>
                    /// Remove duplicates (same accession) from FASTA file
                    /// </summary>
                    public static void RemoveDuplicatesInFastaFile(string fasta_file)
                    {
                        var result_fasta_text = "";
                        var accession_set = new HashSet<string>();
                        var fasta_text = File.ReadAllText(fasta_file);
                        var fasta_parts = Regex.Split(fasta_text, "^>", RegexOptions.Multiline);
                        foreach (var fp in fasta_parts)
                        {
                            if (fp.IsNullOrEmpty())
                            {
                                continue;
                            }
                            var accession = fp.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries)[0];
                            if (!accession_set.Contains(accession))
                            {
                                accession_set.Add(accession);
                                result_fasta_text += ">";
                                result_fasta_text += fp;
                            }
                        }
                        File.WriteAllText(fasta_file, result_fasta_text);
                    }

                    enum ParseState
                    {
                        NONE,
                        PROTEIN_HIT,
                        PEPTIDE_IDENTIFICATION,
                        PEPTIDE_HIT,
                        PEPTIDE_HIT_USERPARAM,
                        PEPTIDE_IDENTIFICATION_USERPARAM
                    };

                    /*
                    /// <summary>
                    /// Parse results in csv_filename and add to EntityDataService
                    /// </summary>
                    private void ParseCSVResults(string csv_filename)
                    {
                        if (EntityDataService.ContainsEntity<NuXLItem>() == false)
                        {
                            EntityDataService.RegisterEntity<NuXLItem>(ProcessingNodeNumber);
                        }

                        var nuxl_items = new List<NuXLItem>();

                        StreamReader reader = File.OpenText(csv_filename);
                        string line = reader.ReadLine(); // ignore header

                        while ((line = reader.ReadLine()) != null)
                        {
                            string[] items = line.Split(new char[] { '\t' }, StringSplitOptions.None);

                            if (items.Length != 40) continue; // skip empty lines

                            var x = new NuXLItem();

                            x.WorkflowID = WorkflowID;
                            x.Id = EntityDataService.NextId<NuXLItem>();

                            double dbl_val;
                            Int32 int_val;

                            x.rt = Double.TryParse(items[0], out dbl_val) ? (dbl_val / 60.0) : 0.0;
                            x.orig_mz = Double.TryParse(items[1], out dbl_val) ? dbl_val : 0.0;
                            x.proteins = items[2];
                            x.peptide = items[3];
                            x.rna = items[4];
                            x.charge = Int32.TryParse(items[5], out int_val) ? int_val : 0;
                            x.score = Double.TryParse(items[6], out dbl_val) ? dbl_val : 0.0;
                            x.best_loc_score = Double.TryParse(items[7], out dbl_val) ? (dbl_val > 1e-20 ? dbl_val * 100.0 : 0.0) : 0.0;
                            x.loc_scores = items[8];
                            x.best_localizations = items[9];
                            x.peptide_weight = Double.TryParse(items[10], out dbl_val) ? dbl_val : 0.0;
                            x.rna_weight = Double.TryParse(items[11], out dbl_val) ? dbl_val : 0.0;
                            x.xl_weight = Double.TryParse(items[12], out dbl_val) ? dbl_val : 0.0;
                            x.a_1 = Double.TryParse(items[13], out dbl_val) ? dbl_val : 0.0;
                            x.a_3 = Double.TryParse(items[14], out dbl_val) ? dbl_val : 0.0;
                            x.c_1 = Double.TryParse(items[15], out dbl_val) ? dbl_val : 0.0;
                            x.c_3 = Double.TryParse(items[16], out dbl_val) ? dbl_val : 0.0;
                            x.g_1 = Double.TryParse(items[17], out dbl_val) ? dbl_val : 0.0;
                            x.g_3 = Double.TryParse(items[18], out dbl_val) ? dbl_val : 0.0;
                            x.u_1 = Double.TryParse(items[19], out dbl_val) ? dbl_val : 0.0;
                            x.u_3 = Double.TryParse(items[20], out dbl_val) ? dbl_val : 0.0;
                            x.abs_prec_error_da = Double.TryParse(items[21], out dbl_val) ? dbl_val : 0.0;
                            x.rel_prec_error_ppm = Double.TryParse(items[22], out dbl_val) ? dbl_val : 0.0;
                            x.m_h = Double.TryParse(items[23], out dbl_val) ? dbl_val : 0.0;
                            x.m_2h = Double.TryParse(items[24], out dbl_val) ? dbl_val : 0.0;
                            x.m_3h = Double.TryParse(items[25], out dbl_val) ? dbl_val : 0.0;
                            x.m_4h = Double.TryParse(items[26], out dbl_val) ? dbl_val : 0.0;
                            x.fragment_annotation = items[39];

                            // don't add unidentified spectra
                            if (x.peptide == "" && x.rna == "")
                            {
                                continue;
                            }

                            nuxl_items.Add(x);
                        }

                        EntityDataService.InsertItems(nuxl_items);

                        // establish connection between results and spectra
                        connectNuXLItemWithSpectra();

                        // add CV column
                        AddCompVoltageToCsm();
                    }
                    */

        public static List<NuXLItem> parseIdXML(string id_file)
        {
            var nuxl_items = new List<NuXLItem>();
            using (XmlReader reader = XmlReader.Create(id_file))
            {
                // Move the reader to first ProteinIdentification
                reader.MoveToContent();
                reader.ReadToDescendant("ProteinHit");

                ParseState s = ParseState.NONE;

                // Parse
                var x = new NuXLItem();
                double dbl_val;
                double spec_mz = 0;
                double spec_rt = 0;
                Int32 int_val;
                string protein_identifier = "UNKNOWN";
                string n = "", v; // name and value of UserParam
                Dictionary<string, string> mapId2Acc = new Dictionary<string, string>(); // protein ID (e.g., PH_1) to protein accession 
                do
                {
                    switch (reader.NodeType)
                    {                        
                        case XmlNodeType.Element:
                            if (reader.Name == "ProteinHit")
                            {
                                s = ParseState.PROTEIN_HIT;
                            }
                            else if (reader.Name == "PeptideIdentification") // starting a new Spectrum?
                            {
                                s = ParseState.PEPTIDE_IDENTIFICATION;
                            }
                            else if (reader.Name == "PeptideHit") // starting a new PSM?
                            {
                                s = ParseState.PEPTIDE_HIT;
                                x = new NuXLItem();
                            }
                            else if (reader.Name == "UserParam" && s == ParseState.PEPTIDE_HIT)
                            {
                                s = ParseState.PEPTIDE_HIT_USERPARAM;
                            }
                            else if (reader.Name == "UserParam" && s == ParseState.PEPTIDE_IDENTIFICATION)
                            {
                                s = ParseState.PEPTIDE_IDENTIFICATION_USERPARAM;
                            }

//                            Console.Write("<{0}", reader.Name);
                            while (reader.MoveToNextAttribute())
                            {
                                switch(s)
                                {
                                    case ParseState.PROTEIN_HIT:
                                        if (reader.Name == "id") { protein_identifier = reader.Value; continue; }
                                        if (reader.Name == "accession") { mapId2Acc.Add(protein_identifier, reader.Value); continue; } // line must to be after id
                                        break;
                                    case ParseState.PEPTIDE_IDENTIFICATION:                                  
                                        if (reader.Name == "MZ") { spec_mz = Double.TryParse(reader.Value, out dbl_val) ? dbl_val : 0.0; continue; }
                                        if (reader.Name == "RT") { spec_rt = Double.TryParse(reader.Value, out dbl_val) ? (dbl_val / 60.0) : 0.0; continue; }
                                        // if (reader.Name == "spectrum_reference") { x.spectrum_reference = reader.Value; continue; }
                                        break;
                                    case ParseState.PEPTIDE_HIT:
                                        if (reader.Name == "score") { x.score = Double.TryParse(reader.Value, out dbl_val) ? dbl_val : 0.0; continue; }
                                        if (reader.Name == "sequence") { x.peptide = reader.Value; continue; }
                                        if (reader.Name == "charge") { x.charge = Int32.TryParse(reader.Value, out int_val) ? int_val : 0; continue; }
/*                                      if (reader.Name == "aa_before") { aa_before = reader.Value; continue; }
                                        if (reader.Name == "aa_after") { aa_after = reader.Value; continue; }
                                        if (reader.Name == "start") { start = reader.Value; continue; }
                                        if (reader.Name == "end") { end = reader.Value; continue; }
*/
                                        if (reader.Name == "protein_refs") // map to protein
                                        { 
                                            string[] protein_refs = reader.Value.Split(' ');
                                            StringBuilder proteinsStringBuilder = new StringBuilder();
                                            foreach (string r in protein_refs)
                                            {
                                                proteinsStringBuilder.Append(mapId2Acc[r] + ';');
                                            }
                                            if (proteinsStringBuilder.Length != 0) proteinsStringBuilder.Length--; // remove last semi-colon
                                            x.proteins = proteinsStringBuilder.ToString();
                                            continue; 
                                        }

                                        x.orig_mz = spec_mz;
                                        x.rt = spec_rt;

                                        break;
                                    case ParseState.PEPTIDE_IDENTIFICATION_USERPARAM:
                                        break;
                                    case ParseState.PEPTIDE_HIT_USERPARAM:
                                        if (reader.Name == "name") { n = reader.Value; continue; }
                                        if (reader.Name == "value") 
                                        { 
                                            v = reader.Value;
                                            if (n == "NuXL:NA") { x.rna = v; continue; }
                                            if (n == "NuXL:best_localization_score") { x.best_loc_score = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:best_localization") { x.best_localizations = v; continue; }
                                            if (n == "NuXL:localization_scores") { x.loc_scores = v; continue; }
                                            if (n == "NuXL:peptide_mass_z0") { x.peptide_weight = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:NA_MASS_z0") { x.rna_weight = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:xl_mass_z0") { x.xl_weight = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:Da difference") { x.abs_prec_error_da = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "precursor_mz_error_ppm") { x.rel_prec_error_ppm = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "fragment_annotation") { x.fragment_annotation = v; continue; }
                                            if (n == "NuXL:NT") { x.nt = v; continue; }

                                            if (n.StartsWith("A_136")) { x.a_1 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("A_330")) { x.a_3 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("C_112")) { x.c_1 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("C_306")) { x.c_3 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("G_152")) { x.g_1 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("G_346")) { x.g_3 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("U_113")) { x.u_1 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n.StartsWith("U_307")) { x.u_3 = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:z1 mass") { x.m_h = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:z2 mass") { x.m_2h = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:z3 mass") { x.m_3h = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }
                                            if (n == "NuXL:z4 mass") { x.m_4h = Double.TryParse(v, out dbl_val) ? dbl_val : 0.0; continue; }

                                            continue;
                                        }

                                        break;
                                    default:
                                        break;
                                }

//                              Console.Write(" {0}='{1}'", reader.Name, reader.Value);
                            }
//                            Console.Write(">");
                            break;
                        case XmlNodeType.Text:
                            Console.Write(reader.Value);
                            break;
                        case XmlNodeType.EndElement:
                            if (reader.Name == "PeptideHit") // finished reading a PSM
                            {
                                nuxl_items.Add(x);
                                x = new NuXLItem();
                            }
//                            Console.Write("</{0}>", reader.Name);
//                            Console.WriteLine("");
                            break;
                    }
                } while (reader.Read());
            }
            return nuxl_items;
        }
    }
}
