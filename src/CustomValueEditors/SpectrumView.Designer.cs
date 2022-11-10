//-----------------------------------------------------------------------------
// Copyright (c) 2003-2015, Thermo Fisher Scientific
// All rights reserved
//-----------------------------------------------------------------------------

using pwiz.MSGraph;

namespace Thermo.Discoverer.SampleNodes.CustomValueEditors
{
    partial class SpectrumView
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.msGraphControl = new MSGraphControl();
            this.SuspendLayout();
            // 
            // msGraphControl
            // 
            this.msGraphControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.msGraphControl.Location = new System.Drawing.Point(0, 0);
            this.msGraphControl.Name = "msGraphControl";
            this.msGraphControl.ScrollGrace = 0D;
            this.msGraphControl.ScrollMaxX = 0D;
            this.msGraphControl.ScrollMaxY = 0D;
            this.msGraphControl.ScrollMaxY2 = 0D;
            this.msGraphControl.ScrollMinX = 0D;
            this.msGraphControl.ScrollMinY = 0D;
            this.msGraphControl.ScrollMinY2 = 0D;
            this.msGraphControl.Size = new System.Drawing.Size(940, 412);
            this.msGraphControl.TabIndex = 1;
            this.msGraphControl.Load += new System.EventHandler(this.msGraphControl_Load);
            // 
            // SpectrumView
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(940, 412);
            this.Controls.Add(this.msGraphControl);
            this.Name = "SpectrumView";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.Text = "Spectrum View";
            this.Load += new System.EventHandler(this.SpectrumView_Load);
            this.ResumeLayout(false);

        }

        #endregion

        private MSGraphControl msGraphControl;


    }
}
